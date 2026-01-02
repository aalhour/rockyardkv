package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// This tool enforces an API hygiene rule for the root package:
// exported API surfaces must not reference internal packages in their signatures.
//
// It intentionally checks syntax (package selectors), not type identity.
// Type aliases are allowed to bridge internal types into nameable public types.
func main() {
	wd, err := os.Getwd()
	if err != nil {
		fatalf("getwd: %v", err)
	}

	goFiles, err := filepath.Glob(filepath.Join(wd, "*.go"))
	if err != nil {
		fatalf("glob: %v", err)
	}
	sort.Strings(goFiles)

	fset := token.NewFileSet()
	var files []*ast.File
	fileImportMaps := make(map[*ast.File]map[string]string)

	for _, path := range goFiles {
		base := filepath.Base(path)
		if strings.HasPrefix(base, ".") {
			// Skip dotfiles like ".go" (can exist as directories in some worktrees).
			continue
		}
		if st, err := os.Stat(path); err == nil && st.IsDir() {
			continue
		}
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			fatalf("parse %s: %v", path, err)
		}
		files = append(files, af)
		fileImportMaps[af] = buildImportMap(af)
	}

	var leaks []leak
	for _, f := range files {
		importMap := fileImportMaps[f]
		leaks = append(leaks, checkFile(fset, f, importMap)...)
	}

	if len(leaks) == 0 {
		fmt.Println("✅ No API leaks detected.")
		return
	}

	sort.Slice(leaks, func(i, j int) bool {
		if leaks[i].file != leaks[j].file {
			return leaks[i].file < leaks[j].file
		}
		if leaks[i].line != leaks[j].line {
			return leaks[i].line < leaks[j].line
		}
		return leaks[i].msg < leaks[j].msg
	})

	fmt.Println("❌ API leaks detected! Please fix before release.")
	for _, l := range leaks {
		fmt.Printf("LEAK: %s:%d: %s\n", l.file, l.line, l.msg)
	}
	os.Exit(1)
}

type leak struct {
	file string
	line int
	msg  string
}

func checkFile(fset *token.FileSet, f *ast.File, importMap map[string]string) []leak {
	var leaks []leak

	// Dot-imports of internal packages are always a leak because internal identifiers can
	// appear without package selectors.
	for name, path := range importMap {
		if name == "." && strings.Contains(path, "/internal/") {
			leaks = append(leaks, leakAt(fset, f.Package, fmt.Sprintf("dot-import of internal package %q", path)))
		}
	}

	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Name == nil || !d.Name.IsExported() {
				continue
			}
			leaks = append(leaks, checkFuncDecl(fset, importMap, d)...)
		case *ast.GenDecl:
			leaks = append(leaks, checkGenDecl(fset, importMap, d)...)
		}
	}

	return leaks
}

func checkFuncDecl(fset *token.FileSet, importMap map[string]string, d *ast.FuncDecl) []leak {
	var leaks []leak
	if d.Type == nil {
		return leaks
	}
	// Exported methods on unexported receiver types are not part of the public API
	// surface. Only check methods whose receiver type is exported.
	if d.Recv != nil {
		if recvName, ok := receiverBaseTypeName(d.Recv); ok && recvName != "" && !ast.IsExported(recvName) {
			return leaks
		}
	}
	leaks = append(leaks, checkExprForInternalSelector(fset, importMap, d.Type, "exported func "+d.Name.Name)...)
	return leaks
}

func checkGenDecl(fset *token.FileSet, importMap map[string]string, d *ast.GenDecl) []leak {
	var leaks []leak
	for _, spec := range d.Specs {
		switch s := spec.(type) {
		case *ast.TypeSpec:
			if s.Name == nil || !s.Name.IsExported() {
				continue
			}
			// Allow exported type aliases. They are the explicit bridge that makes internal
			// types nameable by users without importing internal packages.
			if s.Assign != token.NoPos {
				continue
			}
			// For exported structs, only exported fields and exported embedded fields
			// participate in the public API surface.
			if st, ok := s.Type.(*ast.StructType); ok && st.Fields != nil {
				for _, field := range st.Fields.List {
					if field == nil {
						continue
					}
					// Embedded field: exported if its base type is exported.
					if len(field.Names) == 0 {
						if isExportedTypeExpr(field.Type) {
							leaks = append(leaks, checkExprForInternalSelector(fset, importMap, field.Type, "exported type "+s.Name.Name+" embedded field")...)
						}
						continue
					}
					// Named fields: check only exported field names.
					for _, fn := range field.Names {
						if fn != nil && fn.IsExported() {
							leaks = append(leaks, checkExprForInternalSelector(fset, importMap, field.Type, "exported field "+s.Name.Name+"."+fn.Name)...)
						}
					}
				}
			} else if it, ok := s.Type.(*ast.InterfaceType); ok && it.Methods != nil {
				for _, field := range it.Methods.List {
					if field == nil {
						continue
					}
					// Embedded interface: treat as part of API if the embedded type is exported.
					if len(field.Names) == 0 {
						if isExportedTypeExpr(field.Type) {
							leaks = append(leaks, checkExprForInternalSelector(fset, importMap, field.Type, "exported interface "+s.Name.Name+" embedded")...)
						}
						continue
					}
					for _, mn := range field.Names {
						if mn != nil && mn.IsExported() {
							leaks = append(leaks, checkExprForInternalSelector(fset, importMap, field.Type, "exported interface method "+s.Name.Name+"."+mn.Name)...)
						}
					}
				}
			} else {
				leaks = append(leaks, checkExprForInternalSelector(fset, importMap, s.Type, "exported type "+s.Name.Name)...)
			}

			// Hard denylist for specific “machinery” types that must not be public at root.
			switch s.Name.Name {
			case "LockManager", "BlobFileManager", "BlobGarbageCollector", "FlushJob", "BufferPool":
				leaks = append(leaks, leakAt(fset, s.Name.Pos(), "exported machinery type "+s.Name.Name+" must be internal"))
			}

		case *ast.ValueSpec:
			for _, n := range s.Names {
				if n == nil || !n.IsExported() {
					continue
				}
				// Only check explicit types. Inferred types require full type-checking.
				if s.Type != nil {
					leaks = append(leaks, checkExprForInternalSelector(fset, importMap, s.Type, "exported value "+n.Name)...)
				}
			}
		}
	}
	return leaks
}

func receiverBaseTypeName(fl *ast.FieldList) (string, bool) {
	if fl == nil || len(fl.List) == 0 || fl.List[0] == nil {
		return "", false
	}
	t := fl.List[0].Type
	// Handle pointers.
	if se, ok := t.(*ast.StarExpr); ok {
		t = se.X
	}
	// Handle generic receiver types like T[P] (rare here but safe).
	switch tt := t.(type) {
	case *ast.Ident:
		return tt.Name, true
	case *ast.IndexExpr:
		if id, ok := tt.X.(*ast.Ident); ok {
			return id.Name, true
		}
		return "", false
	case *ast.IndexListExpr:
		if id, ok := tt.X.(*ast.Ident); ok {
			return id.Name, true
		}
		return "", false
	default:
		return "", false
	}
}

func isExportedTypeExpr(expr ast.Expr) bool {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.IsExported()
	case *ast.StarExpr:
		return isExportedTypeExpr(t.X)
	case *ast.SelectorExpr:
		// pkg.Type: exported if Type is exported.
		return t.Sel != nil && ast.IsExported(t.Sel.Name)
	case *ast.IndexExpr:
		return isExportedTypeExpr(t.X)
	case *ast.IndexListExpr:
		return isExportedTypeExpr(t.X)
	default:
		return false
	}
}

func buildImportMap(f *ast.File) map[string]string {
	m := make(map[string]string)
	for _, is := range f.Imports {
		if is.Path == nil {
			continue
		}
		path, err := strconv.Unquote(is.Path.Value)
		if err != nil {
			continue
		}
		name := ""
		if is.Name != nil {
			name = is.Name.Name
		} else {
			name = defaultImportName(path)
		}
		if name == "" {
			continue
		}
		m[name] = path
	}
	return m
}

func defaultImportName(path string) string {
	// Strip any trailing slash.
	path = strings.TrimSuffix(path, "/")
	if path == "" {
		return ""
	}
	if i := strings.LastIndex(path, "/"); i >= 0 && i+1 < len(path) {
		return path[i+1:]
	}
	return path
}

func checkExprForInternalSelector(fset *token.FileSet, importMap map[string]string, expr ast.Expr, context string) []leak {
	var leaks []leak
	ast.Inspect(expr, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkgIdent, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		path, ok := importMap[pkgIdent.Name]
		if !ok {
			return true
		}
		if strings.Contains(path, "/internal/") {
			leaks = append(leaks, leakAt(fset, sel.Pos(), fmt.Sprintf("%s references internal package %q (%s.%s)", context, path, pkgIdent.Name, sel.Sel.Name)))
		}
		return true
	})
	return leaks
}

func leakAt(fset *token.FileSet, pos token.Pos, msg string) leak {
	p := fset.Position(pos)
	return leak{
		file: filepath.Base(p.Filename),
		line: p.Line,
		msg:  msg,
	}
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
