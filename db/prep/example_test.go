package prep

import "fmt"

func ExampleRow() {
	s := struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
		Age  int    `db:"age"`
	}{
		1,
		"Albert",
		42,
	}
	fmt.Println(Row(s, "id", "age", "name"))

	// Output: [1 42 Albert]

}

func ExamplePrepare_Check() {
	s := struct {
		ID          int    `json:"id" db:"-"`
		Name        string `json:"name"`
		Age         int    `json:"age"`
		Address     string `json:"address"`
		PhoneNumber string `json:"phone_number" db:"number"`
	}{
		10,
		"Susan",
		14,
		"123 Main Street",
		"111-222-4567",
	}

	p := New(s)
	err := p.Check("name", "age", "address", "number")
	if err != nil {
		fmt.Println(err)
	}

	err = p.Check("id", "phone_number")
	if err != nil {
		fmt.Println(err)
	}

	// Output: columns not found: id
	// phone_number
}
