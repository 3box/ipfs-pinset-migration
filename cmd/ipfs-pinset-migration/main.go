package main

import (
	"github.com/3box/ipfs-pinset-migration/migrate"
	"github.com/alecthomas/kong"
)

type CliOptions struct {
	S3 struct {
		Bucket string `short:"b" help:"S3 bucket name"`
		Prefix string `short:"p" help:"S3 bucket prefix for pins"`
	} `cmd:"" name:"s3" help:"Migrate S3 pin store"`

	Fs struct {
		Path string `short:"p" help:"Filesystem path for pins"`
	} `cmd:"" name:"fs" help:"Migrate filesystem pin store"`

	IpfsApiUrl string `short:"i" default:"localhost:5001" help:"IPFS API url"`
	LogPath    string `short:"l" default:"/tmp" help:"Path to store log files"`
}

func main() {
	var cli CliOptions
	ctx := kong.Parse(&cli)
	switch ctx.Command() {
	case "s3":
		migrate.Migrate(&migrate.MigrateS3{Bucket: cli.S3.Bucket, Prefix: cli.S3.Prefix}, cli.IpfsApiUrl, cli.LogPath)
		break
	case "fs":
		migrate.Migrate(&migrate.MigrateFs{Path: cli.Fs.Path}, cli.IpfsApiUrl, cli.LogPath)
		break
	default:
		panic(ctx.Command())
	}
}
