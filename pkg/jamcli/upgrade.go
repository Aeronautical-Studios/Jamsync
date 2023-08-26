package jamcli

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"

	"github.com/fynelabs/selfupdate"
	"github.com/zdgeier/jam/pkg/jamsite"
)

func Upgrade() {
	vresp, err := http.Get(jamsite.Host() + "/currentversion")
	if err != nil {
		panic(err)
	}
	defer vresp.Body.Close()

	vb, err := io.ReadAll(vresp.Body)
	if err != nil {
		log.Panic(err)
	}

	resp, err := http.Get(jamsite.Host() + "/clients/" + string(vb) + "/jam_" + runtime.GOOS + "_" + runtime.GOARCH + ".zip")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Panic(err)
	}

	reader := bytes.NewReader(b)
	zipReader, err := zip.NewReader(reader, int64(len(b)))
	if err != nil {
		panic(err)
	}

	f, err := zipReader.Open("jam")
	if err != nil {
		panic(err)
	}

	err = selfupdate.Apply(f, selfupdate.Options{})
	if err != nil {
		panic(err)
	}

	fmt.Println("Updated to version " + string(vb))
}

func Unzip(name string, zippedBytes []byte) ([]byte, error) {
	reader := bytes.NewReader(zippedBytes)
	zipReader, err := zip.NewReader(reader, int64(len(zippedBytes)))
	if err != nil {
		return nil, err
	}
	f, err := zipReader.Open(name)
	if err != nil {
		panic(err)
	}
	p, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return p, nil
}
