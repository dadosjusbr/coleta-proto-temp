package main

import (
	"fmt"
	"log"
	"os"

	"github.com/dadosjusbr/proto"
	"github.com/dadosjusbr/proto/coleta"
	"google.golang.org/protobuf/types/known/timestamppb"
	
	"google.golang.org/protobuf/encoding/protojson"

)

var gitCommit string

const (
	agenciaID  = "mppb"
	repColetor = "https://github.com/dadosjusbr/coletores"
)

func main() {
	/*
	month, err := strconv.Atoi(os.Getenv("MONTH"))
	if err != nil {
		log.Fatalf("Invalid month (\"%s\"): %q", os.Getenv("MONTH"), err)
	}
	year, err := strconv.Atoi(os.Getenv("YEAR"))
	if err != nil {
		log.Fatalf("Invalid year (\"%s\"): %q", os.Getenv("YEAR"), err)
	}
	*/
	month, year := 2, 2020
	outputFolder := os.Getenv("OUTPUT_FOLDER")
	if outputFolder == "" {
		outputFolder = "./output"
	}

	if err := os.Mkdir(outputFolder, os.ModePerm); err != nil && !os.IsExist(err) {
		log.Fatalf("Error creating output folder(%s): %q", outputFolder, err)
	}

	files, err := Crawl(outputFolder, month, year)
	if err != nil {
		log.Fatalf("Crawler error: %q", err)
	}

	chaveColeta := proto.IDColeta(agenciaID, month, year)

	folha, parseErr := Parse(files, chaveColeta)
	if parseErr != nil {
		log.Fatalf("Parsing error: %q", parseErr)
	}

	colRes := coleta.Coleta{
		ChaveColeta:        chaveColeta,
		Orgao:              agenciaID,
		Mes:                int32(month),
		Ano:                int32(year),
		TimestampColeta:    timestamppb.Now(),
		RepositorioColetor: repColetor,
		VersaoColetor:      gitCommit,
		DirColetor:         agenciaID,
		Arquivos:           files,
	}

	rc := coleta.ResultadoColeta{
		Coleta: &colRes,
		Folha:  folha,
	}
	m := protojson.MarshalOptions{
		Indent:          "  ",
		EmitUnpopulated: true,
		UseProtoNames: true,
		UseEnumNumbers: true,
	}
	jsonBytes, _ := m.Marshal(&rc)
	fmt.Println(string(jsonBytes))
	/*
	b, err := json.MarshalIndent(rc, "", "  ")
	if err != nil {
		log.Fatalf("JSON marshaling error: %q", err)
		os.Exit(1)
	}
	
	fmt.Printf("%s", string(b))
	*/
}
