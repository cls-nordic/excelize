package excelize

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkAddRow(b *testing.B) {
	file := NewFile()
	row := make([]Cell, 10)
	for colID := 0; colID < 10; colID++ {
		row[colID] = Cell{
			StyleID: 1,
			Value:   "foo",
		}
	}
	dw, err := file.NewDirectWriter("Sheet1", 8192)
	require.NoError(b, err)
	go dw.WriteTo(io.Discard)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dw.AddRow(row)
	}
	err = dw.Flush()
	assert.NoError(b, err)
	b.SetBytes(dw.bytesWritten)
	b.ReportAllocs()
}

func TestDirectWriter(t *testing.T) {
	t.Run("non-concurrent-writer", func(t *testing.T) {
		file, row, expectedRow := setupTestFileRow()

		dw, err := file.NewDirectWriter("Sheet1", 8192)
		require.NoError(t, err)

		require.NoError(t, dw.SetColWidth(1, 2, 20))
		expectedCols := `<cols><col min="1" max="2" width="20.000000" customWidth="1"/></cols>`

		err = dw.AddRow(row)
		assert.NoError(t, err)
		err = dw.Flush()
		assert.NoError(t, err)

		var out bytes.Buffer
		_, err = dw.WriteTo(&out)
		require.NoError(t, err)
		assert.Contains(t, out.String(), expectedCols)
		assert.Contains(t, out.String(), expectedRow)
	})
	t.Run("concurrent-writer", func(t *testing.T) {
		file, row, expectedRow := setupTestFileRow()

		dw, err := file.NewDirectWriter("Sheet1", 8192)
		require.NoError(t, err)

		var out bytes.Buffer
		ch := make(chan error)
		go func() {
			_, err := dw.WriteTo(&out)
			ch <- err
		}()

		err = dw.AddRow(row)
		assert.NoError(t, err)
		err = dw.Flush()
		assert.NoError(t, err)

		err = <-ch
		require.NoError(t, err)
		assert.Contains(t,
			out.String(),
			expectedRow,
		)
	})
	t.Run("multiple-concurrent-writers", func(t *testing.T) {
		file, row, _ := setupTestFileRow()
		var (
			sheets = 100
			rows   = 100
			dws    = make([]*DirectWriter, sheets)
			err    error
		)

		// setup some sheets with direct writers
		for i := range dws {
			dws[i], err = file.NewDirectWriter("Sheet"+strconv.Itoa(i+1), 512)
			require.NoError(t, err)
		}

		// launch writer on the final zip file to a buffer
		var out bytes.Buffer
		ch := make(chan error)
		go func() {
			_, err := file.WriteTo(&out)
			ch <- err
		}()

		// for each sheet write some rows, and then close it
		for _, dw := range dws {
			for i := 0; i < rows; i++ {
				err = dw.AddRow(row)
				assert.NoError(t, err)
			}
			err = dw.Flush()
			require.NoError(t, err)
		}

		err = <-ch
		require.NoError(t, err)

		// verify all sheets made it into the zip archive
		z, err := zip.NewReader(bytes.NewReader(out.Bytes()), int64(out.Len()))
		assert.NoError(t, err)
		for i := range dws {
			f, err := z.Open("xl/worksheets/sheet" + strconv.Itoa(dws[i].SheetID) + ".xml")
			assert.NoError(t, err)
			if f != nil {
				f.Close()
			}
		}
		// os.WriteFile("test.xlsx", out.Bytes(), os.ModePerm)
	})
}

func setupTestFileRow() (*File, []Cell, string) {
	file := NewFile()
	ts, _ := file.NewStyle(&Style{NumFmt: 22})
	row := []Cell{
		{Value: "foo"},
		// add trailing ws to trigger xml:space
		{Value: "bar "},
		{Value: time.Date(2021, 11, 29, 0, 0, 0, 0, time.UTC), StyleID: ts},
		{Value: 123},
	}
	expected := fmt.Sprintf("<sheetData><row><c t=\"str\"><v>foo</v></c><c xml:space=\"preserve\" t=\"str\"><v>bar </v></c><c s=\"%d\"><v>44529</v></c><c><v>123</v></c></row></sheetData>", ts)
	return file, row, expected
}
