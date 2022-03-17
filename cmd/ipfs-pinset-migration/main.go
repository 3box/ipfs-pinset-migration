package main

import (
	"fmt"

	migrate "github.com/3box/ipfs-pinset-migration/migrate"
)

func main() {
	message := migrate.Hello("Ceramic")
	fmt.Println(message)
	migrate.SetUp()
}
