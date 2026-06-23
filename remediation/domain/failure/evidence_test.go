package failure

import "testing"

func TestCategoryHealable(t *testing.T) {
	cases := map[Category]bool{
		CategoryLogic:          true,
		CategoryTest:           true,
		CategoryUnknown:        true,
		CategoryInfraTransient: false,
	}
	for cat, want := range cases {
		if got := cat.Healable(); got != want {
			t.Errorf("Category(%q).Healable() = %v, want %v", cat, got, want)
		}
	}
}
