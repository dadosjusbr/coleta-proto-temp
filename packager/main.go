package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/dadosjusbr/coletores/status"
	"github.com/frictionlessdata/datapackage-go/datapackage"
)

const (
	coletaFileName      = "coleta.csv"                  // hardcoded in datapackage_descriptor.json
	folhaFileName       = "contra_cheque.csv"           // hardcoded in datapackage_descriptor.json
	remuneracaoFileName = "remuneracao.csv"             // hardcoded in datapackage_descriptor.json
	packageFileName     = "datapackage_descriptor.json" // name of datapackage descriptor
)

func main() {

	outputPath := os.Getenv("OUTPUT_FOLDER")
	if outputPath == "" {
		outputPath = "./"
	}
	var er ExecutionResult
	erIN, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		status.ExitFromError(status.NewError(4, fmt.Errorf("Error reading crawling result: %q", err)))
	}
	if err = json.Unmarshal(erIN, &er.Rc); err != nil {
		status.ExitFromError(status.NewError(5, fmt.Errorf("Error unmarshaling crawling resul from STDIN: %q", err)))
	}

	// Creating CSVs.

	if err := ToCSVFile(er.Rc.Coleta, coletaFileName); err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("Error creating Coleta CSV:%q", err))
		status.ExitFromError(err)
	}

	if err := ToCSVFile(er.Rc.Folha, folhaFileName); err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("Error creating Folha de pagamento CSV:%q", err))
		status.ExitFromError(err)
	}

	if err := ToCSVFile(er.Rc.Remuneracoes, remuneracaoFileName); err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("Error creating Remuneração CSV:%q", err))
		status.ExitFromError(err)
	}

	// Creating package descriptor.
	c, err := ioutil.ReadFile(packageFileName)
	if err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("Error reading datapackge_descriptor.json:%q", err))
		status.ExitFromError(err)
	}

	var desc map[string]interface{}
	if err := json.Unmarshal(c, &desc); err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("Error unmarshaling datapackage descriptor:%q", err))
		status.ExitFromError(err)
	}

	pkg, err := datapackage.New(desc, ".")
	if err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("Error create datapackage:%q", err))
		status.ExitFromError(err)
	}

	// Packing CSV and package descriptor.
	zipName := filepath.Join(outputPath, fmt.Sprintf("%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes))
	if err := pkg.Zip(zipName); err != nil {
		err = status.NewError(status.SystemError, fmt.Errorf("Error zipping datapackage (%s):%q", zipName, err))
		status.ExitFromError(err)
	}

	// Sending results.
	er.Pr = PackagingResult{Package: zipName}
	b, err := json.MarshalIndent(er, "", "  ")
	if err != nil {
		err = status.NewError(status.Unknown, fmt.Errorf("Error marshalling packaging result (%s):%q", zipName, err))
		status.ExitFromError(err)
	}
	fmt.Println(string(b))
}
