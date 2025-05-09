package manipcli

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"text/template"

	"github.com/ghodss/yaml"
	prettyjson "github.com/hokaccha/go-prettyjson"
	"github.com/olekukonko/tablewriter"
	"go.acuvity.ai/elemental"
)

// PrintTable returns a table representation of the given identifiables.
// If attributes are given, only these attributes will be shown as columns,
func PrintTable(identifiables elemental.Identifiables, attributes ...string) (string, error) {
	return formatObjects(outputFormat{output: flagOutputTable, columns: attributes}, false, identifiables.List()...)
}

// PrintYAML returns a yaml representation of the given identifiables.
// If attributes are given, only these attributes will be shown as columns,
func PrintYAML(identifiables elemental.Identifiables, attributes ...string) (string, error) {
	return formatObjects(outputFormat{output: flagOutputYAML, columns: attributes}, false, identifiables.List()...)
}

// PrintJSON returns a json representation of the given identifiables.
// If attributes are given, only these attributes will be shown as columns,
func PrintJSON(identifiables elemental.Identifiables, attributes ...string) (string, error) {
	return formatObjects(outputFormat{output: flagOutputJSON, columns: attributes}, false, identifiables.List()...)
}

// PrintTemplate returns a representation directed by the given template of the given identifiables.
func PrintTemplate(identifiables elemental.Identifiables, template string) (string, error) {
	return formatObjects(outputFormat{output: flagOutputTemplate, template: template}, false, identifiables.List()...)
}

type prepareOutputConfig struct {
	tableCaption string
}

// prepareOutputOption represents an option that can be passed to PrepareOutputFormat.
type prepareOutputOption func(*prepareOutputConfig)

// outputFormat retains all output information
type outputFormat struct {
	formatType   string
	output       string
	template     string
	tableCaption string
	columns      []string
}

// prepareOutputFormat returns an OutputFormat structure that contains output information
func prepareOutputFormat(output string, formatType string, columns []string, template string, opts ...prepareOutputOption) outputFormat {

	cfg := prepareOutputConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	return outputFormat{
		columns:      columns,
		formatType:   formatType,
		output:       output,
		template:     template,
		tableCaption: cfg.tableCaption,
	}
}

// formatEvents prints all events given an output format
func formatEvents(format outputFormat, forceList bool, events ...*elemental.Event) (string, error) {

	objectMaps := make([]map[string]any, 0, len(events))

	for _, event := range events {
		if event.Encoding == elemental.EncodingTypeMSGPACK {
			if err := event.Convert(elemental.EncodingTypeJSON); err != nil {
				return "", err
			}
		}

		var objectMap map[string]any
		if err := remarshal(event, &objectMap); err == nil {
			objectMaps = append(objectMaps, objectMap)
		}
	}

	output, err := formatMaps(format, forceList, objectMaps)
	if err != nil {
		return "", err
	}

	return output, nil
}

// formatObjects prints all identifiable objects given an output format
func formatObjects(format outputFormat, forceList bool, objects ...elemental.Identifiable) (string, error) {

	return formatObjectsStripped(format, false, false, forceList, objects...)
}

// formatObjectsStripped prints all identifiable objects given an output format and eventually strip out some data.
func formatObjectsStripped(format outputFormat, stripReadOnly bool, stripCreationOnly bool, forceList bool, objects ...elemental.Identifiable) (string, error) {

	objectMaps := make([]map[string]any, 0, len(objects))

	for _, object := range objects {
		var objectMap map[string]any
		if err := remarshal(object, &objectMap); err == nil {
			objectMaps = append(objectMaps, objectMap)
		}

		if ats, ok := object.(elemental.AttributeSpecifiable); ok && stripReadOnly {
			for _, spec := range ats.AttributeSpecifications() {
				if spec.ReadOnly {
					delete(objectMap, spec.Name)
				}
			}
		}

		if ats, ok := object.(elemental.AttributeSpecifiable); ok && stripCreationOnly {
			for _, spec := range ats.AttributeSpecifications() {
				if spec.CreationOnly {
					delete(objectMap, spec.Name)
				}
			}
		}
	}

	output, err := formatMaps(format, forceList, objectMaps)
	if err != nil {
		return "", err
	}

	return output, nil
}

func formatMaps(format outputFormat, forceList bool, objects []map[string]any) (string, error) {

	switch format.output {
	case flagOutputNone:
		return formatObjectsInNone(format, objects...)

	case flagOutputTable:
		if len(objects) == 1 && !forceList {
			return formatSingleObjectInTable(format, objects[0])
		}
		return formatObjectsInTable(format, objects)

	case flagOutputJSON:
		return formatObjectsWithMarshaler(format, objects, prettyjson.Marshal)

	case flagOutputYAML:
		return formatObjectsWithMarshaler(format, objects, yaml.Marshal)

	case flagOutputTemplate:
		if len(objects) == 1 && !forceList {
			return formatSingleObjectWithTemplate(objects[0], format.template)
		}
		return formatObjectsWithTemplate(objects, format.template)

	default:
		panic(fmt.Sprintf("invalid output format '%s'", format))
	}
}

func formatObjectsInNone(format outputFormat, objects ...map[string]any) (string, error) {

	var ids []string // nolint: prealloc

	for _, object := range objects {
		if format.formatType == formatTypeCount {
			return fmt.Sprintf("%d", object[formatTypeCount]), nil
		}

		id, ok := object["ID"].(string)
		if !ok {
			continue
		}

		ids = append(ids, id)
	}

	return strings.Join(ids, "\n"), nil
}

func formatObjectsWithMarshaler(format outputFormat, objects []map[string]any, marshal func(any) ([]byte, error)) (string, error) {

	if len(objects) == 0 {
		if format.output == flagOutputJSON {
			if format.formatType == formatTypeHash || format.formatType == "" {
				return "{}", nil
			}
			return "[]", nil
		}
		return "", nil
	}

	var target any
	// Print objects as an array only when multiple objects are to be printed
	if len(objects) == 1 && (format.formatType == formatTypeHash || format.formatType == formatTypeCount || format.formatType == "") {
		target = objects[0]
	} else {
		target = objects
	}

	output, err := marshal(target)
	if err != nil {
		return "", err
	}

	return string(output), nil
}

// formatObjectsWithTemplate formats the given []map[string]any using given template.
func formatObjectsWithTemplate(obj []map[string]any, tpl string) (string, error) {

	t, err := template.New("tpl").Parse(tpl)
	if err != nil {
		return "", fmt.Errorf("unable to parse template: %s", err)
	}

	buffer := bytes.NewBuffer(nil)
	if err := t.Execute(buffer, obj); err != nil {
		return "", fmt.Errorf("unable to execute template: %s", err)
	}

	return buffer.String(), nil
}

// formatSingleObjectWithTemplate formats the given map[string]any using given template.
func formatSingleObjectWithTemplate(obj map[string]any, tpl string) (string, error) {

	t, err := template.New("tpl").Parse(tpl)
	if err != nil {
		return "", fmt.Errorf("unable to parse template: %s", err)
	}

	buffer := bytes.NewBuffer(nil)
	if err := t.Execute(buffer, obj); err != nil {
		return "", fmt.Errorf("unable to execute template: %s", err)
	}

	return buffer.String(), nil
}

// listFields validates the given "columns" arg with the given object, and
// returns the validated column list.
// If `columns` is empty, it lists all fields from the object and makes "ID"
// frontmost if found.
// If `columns` is non-empty, it filters the columns with what are found in the
// object, keeping the order.
func listFields(object map[string]any, columns []string) []string {

	if len(columns) == 0 {

		keys := make([]string, 0, len(object))

		for k := range object {
			if k != "ID" {
				keys = append(keys, k)
			}
		}

		sort.Strings(keys)

		// Keep "ID" the first field if possible
		if _, ok := object["ID"]; ok {
			keys = append([]string{"ID"}, keys...)
		}

		return keys
	}

	keys := make([]string, 0, len(columns))
	for _, k := range columns {
		if _, ok := object[k]; ok {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)
	return keys
}

func tabulate(header []string, rows [][]string, single bool, caption string) string {

	out := &bytes.Buffer{}

	// colors := make([]tablewriter.Colors, len(header))
	// for i := 0; i < len(header); i++ {
	// 	colors[i] = tablewriter.Color(tablewriter.FgCyanColor, tablewriter.Bold)
	// }

	table := tablewriter.NewWriter(out)
	table.SetHeader(header)
	table.AppendBulk(rows)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderLine(true)
	table.SetBorders(tablewriter.Border{Left: false, Top: false, Right: false, Bottom: false})
	// table.SetHeaderColor(colors...)

	if single {
		table.SetColumnAlignment([]int{tablewriter.ALIGN_RIGHT, tablewriter.ALIGN_LEFT})
	}

	if caption != "" {
		table.SetCaption(true, caption)
	}

	table.Render()

	return "\n" + out.String()
}

func formatSingleObjectInTable(fromat outputFormat, object map[string]any) (string, error) {

	fields := listFields(object, fromat.columns)
	data := make([][]string, len(fields))

	for fieldIdx, field := range fields {
		data[fieldIdx] = []string{field, fmt.Sprintf("%v", object[field])}
	}

	return tabulate([]string{"property", "value"}, data, true, fromat.tableCaption), nil
}

func formatObjectsInTable(format outputFormat, objects []map[string]any) (string, error) {

	if len(objects) == 0 {
		return "", nil
	}

	fields := listFields(objects[0], format.columns)
	data := make([][]string, len(objects))

	for objectIdx, object := range objects {
		objectData := make([]string, len(fields))
		for fieldIdx, field := range fields {
			objectData[fieldIdx] = fmt.Sprintf("%v", object[field])
		}

		data[objectIdx] = objectData
	}

	return tabulate(fields, data, false, format.tableCaption), nil
}

// remarshal marshals an object into a JSON string, and unmarshal it back to the
// given object. If object is not given, a new map[string]any is used.
// The object is returned if no error occurs; otherwise nil with the error.
func remarshal(object any, target any) error {

	if target == nil {
		return fmt.Errorf("Unable to call remarshall on a nil target")
	}

	if encodable, ok := object.(elemental.Encodable); ok {
		buf, err := elemental.Encode(encodable.GetEncoding(), object)
		if err != nil {
			return err
		}
		return elemental.Decode(encodable.GetEncoding(), buf, target)

	}

	buf, err := elemental.Encode(elemental.EncodingTypeMSGPACK, object)
	if err != nil {
		return err
	}

	return elemental.Decode(elemental.EncodingTypeMSGPACK, buf, target)
}

var wg sync.WaitGroup
var realStdout *os.File
var wPipe *os.File

// FlushOutputAndExit provides a way to ensure we flush our pipe and write to stdout before exiting the program.
// Needs to be called before returning from main and anywhere os.Exit is called.
func flushOutputAndExit(code int) {
	os.Stdout = realStdout
	_ = wPipe.Close()
	wg.Wait()
	os.Exit(code)
}
