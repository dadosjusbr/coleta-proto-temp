package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/dadosjusbr/coletores/status"
	csvProto "github.com/dadosjusbr/proto/csv"
	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/frictionlessdata/tableschema-go/csv"
)

var resources = []string{"coleta", "contra_cheque", "remuneracao"}

func main() {
	// Reading input.
	in, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		err := status.NewError(status.InvalidInput, fmt.Errorf("Error reading execution result from stdin: %q", err))
		status.ExitFromError(err)
	}
	var er ExecutionResult
	if err := json.Unmarshal(in, &er); err != nil {
		err := status.NewError(status.InvalidInput, fmt.Errorf("Error unmarshalling execution result: %q", err))
		status.ExitFromError(err)
	}

	// Loading and validating package.
	pkg, err := datapackage.Load(er.Pr.Package)
	if err != nil {
		err = status.NewError(status.DataUnavailable, fmt.Errorf("Error loading datapackage (%s):%q", er.Pr.Package, err))
		status.ExitFromError(err)
	}

	for _, v := range resources {
		sch, err := pkg.GetResource(v).GetSchema()
		if err != nil {
			err = status.NewError(status.DataUnavailable, fmt.Errorf("Error getting schema from data package resource (%s | %s):%q", er.Pr.Package, v, err))
			status.ExitFromError(err)
		}
		if err := sch.Validate(); err != nil {
			err = status.NewError(status.InvalidInput, fmt.Errorf("Error validating schema (%s):%q", er.Pr.Package, err))
			status.ExitFromError(err)
		}

		switch v {
		case "coleta":
			if err := pkg.GetResource(v).Cast(&[]csvProto.Coleta_CSV{}, csv.LoadHeaders()); err != nil {
				err = status.NewError(status.InvalidInput, fmt.Errorf("Error validating datapackage (%s):%q", er.Pr.Package, err))
				status.ExitFromError(err)
			}
		case "remuneracao":
			if err := pkg.GetResource(v).Cast(&[]csvProto.Remuneracao_CSV{}, csv.LoadHeaders()); err != nil {
				err = status.NewError(status.InvalidInput, fmt.Errorf("Error validating datapackage (%s):%q", er.Pr.Package, err))
				status.ExitFromError(err)
			}
		default:
			if err := pkg.GetResource(v).Cast(&[]csvProto.ContraCheque_CSV{}, csv.LoadHeaders()); err != nil {
				err = status.NewError(status.InvalidInput, fmt.Errorf("Error validating datapackage (%s):%q", er.Pr.Package, err))
				status.ExitFromError(err)
			}
		}

	}

	// Printing output.
	out, err := json.MarshalIndent(er, "", "  ")
	if err != nil {
		err = status.NewError(status.OutputError, fmt.Errorf("Error marshaling output:%q", err))
		status.ExitFromError(err)
	}
	fmt.Print(string(out))
}
