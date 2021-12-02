package excelize

import (
	"bytes"
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
	cols          string
	worksheet     *xlsxWorksheet
	sheetPath     string
	maxBufferSize int
	bytesWritten  int64
	buf           []byte
	out           io.Writer
	done          chan bool
	rowCount      int
	maxColLengths []int
	waitMode      bool
}

// NewDirectWriter return a new DirectWriter for the given sheet name. If the sheet doesn't yet exists it is created.
// Similar limitations apply as when using the StreamWriter. To enable writing an xlsx file concurrently to
// a io.Writer you must:
//
// - create a File.
//
// - create at least one DirectWriter.
//
// - launch writing using file.WriteTo() in a separate goroutine, this call will block until all direct writers are closed.
//
// - add data using AddRow, then call Close.
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

	return dw, err
}

// SetWait enables or disables the wait mode. In wait mode nothing is flushed to writer (if any), even if the buffer grows beyond maxBufferSize.
func (dw *DirectWriter) SetWait(b bool) error {
	if b {
		if dw.bytesWritten > 0 {
			return errors.New("Can't enable wait mode since first data already written.")
		}
		dw.waitMode = true
		return nil
	}
	dw.waitMode = false
	return nil
}

// AddRow is used for streaming a large data file row by row, without any gaps.
// It omits  cell reference values and only accept []Cell to reduce interface{} related allocations.
// It returns the number of bytes currently in the write buffer.
func (dw *DirectWriter) AddRow(values []Cell, opts ...RowOpts) (buffered int, err error) {
	dw.rowCount++
	dw.buf = append(dw.buf, `<row r="`...)
	dw.buf = strconv.AppendInt(dw.buf, int64(dw.rowCount), 10)
	dw.buf = append(dw.buf, '"')
	if len(opts) > 0 {
		attrs, err := marshalRowAttrs(opts...)
		if err != nil {
			return len(dw.buf), err
		}
		dw.buf = append(dw.buf, attrs...)
	}
	dw.buf = append(dw.buf, '>')
	if len(values) > len(dw.maxColLengths) {
		l := make([]int, len(values))
		copy(l, dw.maxColLengths)
		dw.maxColLengths = l
	}
	for i, val := range values {
		c := xlsxC{
			S: val.StyleID,
		}
		if val.Formula != "" {
			c.F = &xlsxF{Content: val.Formula}
		}
		if err := setCellValFunc(&c, val.Value); err != nil {
			dw.buf = append(dw.buf, "</row>"...)
			return len(dw.buf), err
		}
		if l := len(c.V); l > dw.maxColLengths[i] {
			dw.maxColLengths[i] = l
		}
		dw.buf = appendCellNoRef(dw.buf, c)
	}
	dw.buf = append(dw.buf, "</row>"...)
	if len(dw.buf) > dw.maxBufferSize && !dw.waitMode {
		err := dw.tryFlush()
		return len(dw.buf), err
	}
	return len(dw.buf), nil
}

// MaxColumnLengths returns the max lengths (in bytes as written to XML) for each column written so far.
func (dw *DirectWriter) MaxColumnLengths() []int {
	return dw.maxColLengths
}

// SetColWidth provides a function to set the width of a single column or
// multiple columns for the DirectWriter. Since column definitions need to be written before sheet data, either use this
// function before the first call to AddRow, or set the writer in wait mode using SetWait.
func (dw *DirectWriter) SetColWidth(min, max int, width float64) error {
	if dw.bytesWritten > 0 {
		return errors.New("Can't set col width since first data already written.")
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

// Close ends the streaming writing process.
func (dw *DirectWriter) Close() error {
	dw.buf = append(dw.buf, `</sheetData>`...)
	bulkAppendFields(dw, dw.worksheet, 8, 15)
	bulkAppendFields(dw, dw.worksheet, 17, 38)
	bulkAppendFields(dw, dw.worksheet, 40, 40)
	dw.buf = append(dw.buf, `</worksheet>`...)

	if err := dw.tryFlush(); err != nil {
		return err
	}

	dw.File.Sheet.Delete(dw.sheetPath)
	delete(dw.File.checked, dw.sheetPath)
	dw.File.Pkg.Delete(dw.sheetPath)

	close(dw.done)
	return nil
}

// WriteTo writes the output of the DirectWriter to w. The call will block until the DirectWriter is closed by a call to Close.
func (dw *DirectWriter) WriteTo(w io.Writer) (int64, error) {
	select {
	case <-dw.done:
		if dw.bytesWritten > 0 {
			return 0, errors.New("Cant't write to new writer w since part of the data already been written and flushed.")
		}
		n, err := w.Write(dw.buildHeader())
		if err != nil {
			return int64(n), err
		}
		n2, err := w.Write(dw.buf)
		return int64(n + n2), err
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

func (dw *DirectWriter) buildHeader() []byte {
	var header bytes.Buffer
	header.WriteString(XMLHeader + `<worksheet` + templateNamespaceIDMap)
	bulkAppendFields(&header, dw.worksheet, 2, 5)
	if len(dw.cols) > 0 {
		header.WriteString("<cols>" + dw.cols + "</cols>")
	}
	header.WriteString(`<sheetData>`)
	return header.Bytes()
}

func (dw *DirectWriter) tryFlush() error {
	dw.Lock()
	if dw.out == nil {
		dw.Unlock()
		return nil
	}
	if dw.bytesWritten == 0 {
		n, err := dw.out.Write(dw.buildHeader())
		if err != nil {
			return err
		}
		dw.bytesWritten += int64(n)
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

func appendCellNoRef(dst []byte, c xlsxC) []byte {
	dst = append(dst, `<c`...)
	if c.XMLSpace.Value != "" {
		dst = append(dst, ` xml:`...)
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
