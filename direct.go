package excelize

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
)

// DirectWriter is a simpler and optimized version of the StreamWriter. Its primary use is sending large amount of sheet data row by row directly
// to a io.Writer. The typical use case is an API writing directly to a TCP connection, with minimal server side buffering.
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
	rowCount      int
}

// NewDirectWriter return a new DirectWriter for the given sheet name. If the sheet doesn't yet exists it is created.
// Similar limitations apply as when using the StreamWriter. To enable writing an xlsx file concurrently to
// a io.Writer you must:
//
// - create a File.
//
// - create at least one DirectWriter.
//
// - launch writing using file.WriteTo() in a separate goroutine, this call will block until all direct writers are flushed.
//
// - add data using AddRow, then call Flush to close it.
//
// - wait for the goroutine to return
func (f *File) NewDirectWriter(sheet string, maxBufferSize int) (*DirectWriter, error) {
	_ = f.NewSheet(sheet)
	sheetID := f.getSheetID(sheet)
	if sheetID == -1 {
		return nil, errors.New("bug: sheetID not found after call to NewSheet")
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

// AddRow is used when streaming a large data file row by row without any gaps.
// It omits any individual row or cell reference values and only accept []Cell to reduce interface{} related allocations.
func (dw *DirectWriter) AddRow(values []Cell, opts ...RowOpts) error {
	dw.rowCount++
	if !dw.sheetWritten {
		if len(dw.cols) > 0 {
			dw.writeString("<cols>" + dw.cols + "</cols>")
		}
		dw.writeString(`<sheetData>`)
		dw.sheetWritten = true
	}
	dw.writeString(`<row`)
	if len(opts) > 0 {
		attrs, err := marshalRowAttrs(opts...)
		if err != nil {
			return err
		}
		dw.writeString(attrs)
	}
	dw.writeString(`>`)
	for _, val := range values {
		c := xlsxC{
			S: val.StyleID,
		}
		if val.Formula != "" {
			c.F = &xlsxF{Content: val.Formula}
		}
		if err := setCellValFunc(&c, val.Value); err != nil {
			dw.writeString(`</row>`)
			return err
		}
		dw.buf = appendCellNoRef(dw.buf, c)
	}
	dw.writeString(`</row>`)
	if len(dw.buf) > dw.maxBufferSize {
		return dw.tryFlush()
	}
	return nil
}

// SetColWidth provides a function to set the width of a single column or
// multiple columns for the DirectWriter. Note that you must call
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

// Flush ends the streaming writing process.
func (dw *DirectWriter) Flush() error {
	if !dw.sheetWritten {
		dw.writeString(`<sheetData>`)
		dw.sheetWritten = true
	}
	dw.writeString(`</sheetData>`)
	bulkAppendFields(dw, dw.worksheet, 8, 15)
	bulkAppendFields(dw, dw.worksheet, 17, 38)
	bulkAppendFields(dw, dw.worksheet, 40, 40)
	dw.writeString(`</worksheet>`)

	if err := dw.tryFlush(); err != nil {
		return err
	}

	dw.File.Sheet.Delete(dw.sheetPath)
	delete(dw.File.checked, dw.sheetPath)
	dw.File.Pkg.Delete(dw.sheetPath)

	close(dw.done)
	return nil
}

// WriteTo writes the output of the DirectWriter to w. The call will block until the DirectWriter is closed by a call to Flush.
func (dw *DirectWriter) WriteTo(w io.Writer) (int64, error) {
	select {
	case <-dw.done:
		// in case stream is already done, write buffer now
		n, err := w.Write(dw.buf)
		return int64(n), err
	default:
		dw.Lock()
		dw.out = w
		dw.Unlock()
		<-dw.done
		return dw.bytesWritten, nil
	}
}

func (dw *DirectWriter) Write(p []byte) (n int, err error) {
	dw.buf = append(dw.buf, p...)
	return len(p), nil
}

func (dw *DirectWriter) tryFlush() error {
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
	if c.S != 0 {
		dst = append(dst, ` s="`...)
		dst = strconv.AppendInt(dst, int64(c.S), 10)
		dst = append(dst, '"')
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
