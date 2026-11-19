package conformance

import "reflect"

// clone returns a deep copy of f: every map, slice and pointer it holds, at
// any depth, is a copy of its own, so a change to the copy never reaches f.
func (f Fixture) clone() Fixture {
	return deepCopy(f)
}

// deepCopy returns a copy of v that shares no map, slice or pointer with it. A
// struct is copied whole and then each exported field is copied again, so an
// unexported field keeps its value.
func deepCopy[T any](v T) T {
	rv := reflect.ValueOf(&v).Elem()
	out, _ := copyValue(rv).Interface().(T)
	return out
}

func copyValue(v reflect.Value) reflect.Value {
	out := reflect.New(v.Type()).Elem()
	switch v.Kind() {
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		out.Set(reflect.MakeSlice(v.Type(), v.Len(), v.Len()))
		for i := range v.Len() {
			out.Index(i).Set(copyValue(v.Index(i)))
		}
	case reflect.Array:
		for i := range v.Len() {
			out.Index(i).Set(copyValue(v.Index(i)))
		}
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		out.Set(reflect.MakeMapWithSize(v.Type(), v.Len()))
		for _, k := range v.MapKeys() {
			out.SetMapIndex(k, copyValue(v.MapIndex(k)))
		}
	case reflect.Pointer:
		if v.IsNil() {
			return v
		}
		p := reflect.New(v.Type().Elem())
		p.Elem().Set(copyValue(v.Elem()))
		out.Set(p)
	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		out.Set(copyValue(v.Elem()))
	case reflect.Struct:
		out.Set(v)
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				out.Field(i).Set(copyValue(v.Field(i)))
			}
		}
	default:
		out.Set(v)
	}
	return out
}
