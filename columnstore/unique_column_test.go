package columnstore

import (
	"testing"

	"github.com/phantom820/collections/types"
	"github.com/stretchr/testify/assert"
)

func TestUniqueInsert(t *testing.T) {

	c := NewUniqueColumn[types.Int]("Marks", 2)
	c.Insert(1)
	assert.ElementsMatch(t, []types.Int{1}, c.Stream().Collect())
	c.Insert(2)
	assert.ElementsMatch(t, []types.Int{1, 2}, c.Stream().Collect())
	c.Insert(3)
	assert.ElementsMatch(t, []types.Int{1, 2, 3}, c.Stream().Collect())
	c.Insert(3)
	assert.ElementsMatch(t, []types.Int{1, 2, 3}, c.Stream().Collect())
	assert.Equal(t, 3, c.Len())

}

func TestUniqueAll(t *testing.T) {

	c := NewUniqueColumn[types.Int]("Marks", 2)

	assert.Equal(t, []types.Int{}, c.All())
	for i := 0; i < 10; i++ {
		c.Insert(types.Int(i))
	}
	assert.ElementsMatch(t, []types.Int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, c.All())

}

func TestUniqueDelete(t *testing.T) {

	c := NewUniqueColumn[types.Int]("Marks", 2)
	for i := 0; i < 6; i++ {
		c.Insert(types.Int(i))
	}
	assert.Equal(t, false, c.Delete(-1))
	assert.Equal(t, true, c.Delete(1))
	assert.ElementsMatch(t, []types.Int{0, 2, 3, 4, 5}, c.Stream().Collect())
	c.Delete(2)
	assert.ElementsMatch(t, []types.Int{0, 3, 4, 5}, c.Stream().Collect())
	c.Delete(4)
	assert.ElementsMatch(t, []types.Int{0, 3, 5}, c.Stream().Collect())

}

func TestUniqueDeleteWhere(t *testing.T) {

	c := NewUniqueColumn[types.Int]("Marks", 2)
	for i := 0; i < 6; i++ {
		c.Insert(types.Int(i))
	}
	c.DeleteWhere(func(x types.Int) bool { return x < 0 })
	assert.ElementsMatch(t, []types.Int{0, 1, 2, 3, 4, 5}, c.Stream().Collect())
	c.DeleteWhere(func(x types.Int) bool { return x > 2 && x < 5 })
	assert.ElementsMatch(t, []types.Int{0, 1, 2, 5}, c.Stream().Collect())

}

func TestUniqueFindWhere(t *testing.T) {

	c := NewUniqueColumn[types.Int]("Marks", 2)
	for i := 0; i < 6; i++ {
		c.Insert(types.Int(i))
	}
	assert.ElementsMatch(t, []types.Int{}, c.FindWhere(func(x types.Int) bool { return x < 0 }))
	assert.ElementsMatch(t, []types.Int{3, 4}, c.FindWhere(func(x types.Int) bool { return x > 2 && x < 5 }))

}

func TestUniqueUpdateWhere(t *testing.T) {

	c := NewUniqueColumn[types.Int]("Marks", 2)
	for i := 0; i < 6; i++ {
		c.Insert(types.Int(i))
	}
	c.UpdateWhere(func(x types.Int) bool { return x == 1 }, func(y types.Int) types.Int { return y - 10 })
	assert.ElementsMatch(t, []types.Int{0, -9, 2, 3, 4, 5}, c.Stream().Collect())
	c.UpdateWhere(func(x types.Int) bool { return x > 1 }, func(y types.Int) types.Int { return y * -1 })
	assert.ElementsMatch(t, []types.Int{0, -9, -2, -3, -4, -5}, c.Stream().Collect())

}
