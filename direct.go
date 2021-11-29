package excelize

import (
	"fmt"
	"io"
	"strconv"
	"sync"
)

// DirectWriter is a simpler and optimized version of the StreamWriter. Its primary use is sending large amount of sheet data row by row directly
// to a io.Writer. Typical usecase is an API writing directly to a TCP connection, with minimal server side buffering.
type DirectWriter struct {
	sync.RWMutex
	File          *File
	Sheet         string
	SheetID       int
	sheetWritten  bool
	cols          string
	worksheet     *xlsxWorksheet
	sheetPath     string
	maxBufferSize int
	bytesWritten  int64
	buf           []byte
	out           io.Writer
	done          chan bool
}

// NewDirectWriter return a direct writer struct by given worksheet name for
// generate new worksheet with large amounts of data. Similar limitations apply
// as when using the StreamWriter. To enable writing an xlsx file concurrently to
// a io.Writer you must:
// - create a File
// - create at least one DirectWriter
// - launch writing using file.WriteTo() in a separate goroutine, this call will block until all direct writers are flushed.
// - add data to direct writers using AddRow. then call Flush to close it.
// - wait for the goroutine to return
func (f *File) NewDirectWriter(sheet string, maxBufferSize int) (*DirectWriter, error) {
	sheetID := f.getSheetID(sheet)
	if sheetID == -1 {
		return nil, fmt.Errorf("sheet %s is not exist", sheet)
	}
	dw := &DirectWriter{
		File:          f,
		Sheet:         sheet,
		SheetID:       sheetID,
		maxBufferSize: maxBufferSize,
		done:          make(chan bool),
	}
	var err error
	dw.worksheet, err = f.workSheetReader(sheet)
	if err != nil {
		return nil, err
	}

	dw.sheetPath = f.sheetMap[trimSheetName(sheet)]
	f.directWriters = append(f.directWriters, dw)

	dw.writeString(XMLHeader + `<worksheet` + templateNamespaceIDMap)
	bulkAppendFields(dw, dw.worksheet, 2, 5)
	return dw, err
}

// AddRow is an optimized version of SetRow useful when streaming a large data file row by raw without any gaps.
// It omits any individual row or cell reference values and only accept []Cell to reduce interface{} related allocations.
func (dw *DirectWriter) AddRow(values []Cell, opts ...RowOpts) error {
	if !dw.sheetWritten {
		if len(dw.cols) > 0 {
			dw.writeString("<cols>" + dw.cols + "</cols>")
		}
		dw.writeString(`<sheetData>`)
		dw.sheetWritten = true
	}
	attrs, err := marshalRowAttrs(opts...)
	if err != nil {
		return err
	}
	fmt.Fprintf(dw, `<row%s>`, attrs)
	for _, val := range values {
		c := xlsxC{
			S: val.StyleID,
		}
		setCellFormula(&c, val.Formula)
		if err = setCellValFunc(&c, val.Value); err != nil {
			dw.writeString(`</row>`)
			return err
		}
		dw.buf = appendCellNoRef(dw.buf, c)
	}
	dw.writeString(`</row>`)
	if len(dw.buf) > dw.maxBufferSize {
		dw.flush()
	}
	return nil
}

// SetColWidth provides a function to set the width of a single column or
// multiple columns for the StreamWriter. Note that you must call
// the 'SetColWidth' function before any call to 'AddRow' function. For example set
// the width column B:C as 20:
//
//    err := directwriter.SetColWidth(2, 3, 20)
//
func (dw *DirectWriter) SetColWidth(min, max int, width float64) error {
	if dw.sheetWritten {
		return ErrStreamSetColWidth
	}
	if min > TotalColumns || max > TotalColumns {
		return ErrColumnNumber
	}
	if min < 1 || max < 1 {
		return ErrColumnNumber
	}
	if width > MaxColumnWidth {
		return ErrColumnWidth
	}
	if min > max {
		min, max = max, min
	}
	dw.cols += fmt.Sprintf(`<col min="%d" max="%d" width="%f" customWidth="1"/>`, min, max, width)
	return nil
}

// Flush ending the streaming writing process.
func (dw *DirectWriter) Flush() error {
	if err := dw.flush(); err != nil {
		return err
	}
	if !dw.sheetWritten {
		_, _ = dw.WriteString(`<sheetData>`)
		dw.sheetWritten = true
	}
	_, _ = dw.WriteString(`</sheetData>`)
	bulkAppendFields(dw, dw.worksheet, 8, 15)
	bulkAppendFields(dw, dw.worksheet, 17, 38)
	bulkAppendFields(dw, dw.worksheet, 40, 40)
	_, _ = dw.WriteString(`</worksheet>`)

	dw.File.Sheet.Delete(dw.sheetPath)
	delete(dw.File.checked, dw.sheetPath)
	dw.File.Pkg.Delete(dw.sheetPath)

	close(dw.done)
	return nil
}

func (dw *DirectWriter) WriteTo(w io.Writer) (int64, error) {
	dw.Lock()
	dw.out = w
	dw.Unlock()
	<-dw.done
	return dw.bytesWritten, nil
}

func (dw *DirectWriter) flush() error {
	dw.Lock()
	if dw.out == nil {
		dw.Unlock()
		return nil
	}
	n, err := dw.out.Write(dw.buf)
	dw.Unlock()
	if err != nil {
		return err
	}
	dw.bytesWritten += int64(n)
	dw.buf = dw.buf[:0]
	return nil
}

func (dw *DirectWriter) Write(p []byte) (n int, err error) {
	dw.buf = append(dw.buf, p...)
	return len(p), nil
}

func (dw *DirectWriter) WriteString(s string) (n int, err error) {
	dw.buf = append(dw.buf, s...)
	return len(s), nil
}

func (dw *DirectWriter) writeString(s string) {
	dw.buf = append(dw.buf, s...)
}

func appendCellNoRef(dst []byte, c xlsxC) []byte {
	dst = append(dst, `<c`...)
	if c.XMLSpace.Value != "" {
		dst = append(dst, ` xml:"`...)
		dst = append(dst, c.XMLSpace.Name.Local...)
		dst = append(dst, `="`...)
		dst = append(dst, c.XMLSpace.Value...)
		dst = append(dst, '"')
	}
	if c.S > 1 {
		dst = append(dst, ` s="`...)
		dst = strconv.AppendInt(dst, int64(c.S), 10)
		dst = append(dst, `"`...)
	}
	if c.T != "" {
		dst = append(dst, ` t="`...)
		dst = append(dst, c.T...)
		dst = append(dst, '"')
	}
	dst = append(dst, '>')
	if c.F != nil {
		dst = append(dst, `<f>`...)
		dst = appendEscapedString(dst, c.F.Content, true)
		dst = append(dst, `</f>`...)
	}
	if c.V != "" {
		dst = append(dst, `<v>`...)
		dst = appendEscapedString(dst, c.V, true)
		dst = append(dst, `</v>`...)
	}
	dst = append(dst, `</c>`...)
	return dst
}
