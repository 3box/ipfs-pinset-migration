package main

import (
    "fmt"

    migrate "github.com/3box/ipfs-pinset-migration"
)

func main() {
    message := migrate.Hello("Ceramic")
    fmt.Println(message)
}

