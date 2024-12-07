// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package record_test

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

func TestRecodeCodec(t *testing.T) {
	s := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	rec := genRowRec(s,
		[]int{0, 1, 1, 1}, []int64{0, 2, 3, 4},
		[]int{1, 0, 1, 1}, []float64{1, 0, 3, 4},
		[]int{1, 1, 0, 1}, []string{"a", "b", "", "d"},
		[]int{1, 1, 1, 0}, []bool{true, true, true, false},
		[]int64{1, 2, 3, 4})

	var err error
	pc := make([]byte, 0, rec.CodecSize())
	pc, err = rec.Marshal(pc)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(pc) != rec.CodecSize() {
		t.Fatalf("error size, exp: %d; got: %d", len(pc), rec.CodecSize())
	}

	newRec := &record.Record{}
	err = newRec.Unmarshal(pc)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if !reflect.DeepEqual(rec.Schema, newRec.Schema) {
		t.Fatalf("marshal schema failed")
	}

	for i := 0; i < rec.Len(); i++ {
		if !reflect.DeepEqual(rec.ColVals[i], newRec.ColVals[i]) {
			t.Fatal("marshal colVal failed")
		}
	}
}

func generateLargeRecord() (r *record.Record) {
	const schemaLen = 4
	const recLen = 800
	s := record.Schemas{}
	for i := 0; i < schemaLen; i++ {
		s = append(s, record.Field{Type: influx.Field_Type_Int, Name: "int" + strconv.Itoa(i)})
		s = append(s, record.Field{Type: influx.Field_Type_Float, Name: "float" + strconv.Itoa(i)})
		s = append(s, record.Field{Type: influx.Field_Type_Boolean, Name: "boolean" + strconv.Itoa(i)})
		s = append(s, record.Field{Type: influx.Field_Type_String, Name: "boolean" + strconv.Itoa(i)})
	}
	s = append(s, record.Field{Type: influx.Field_Type_Int, Name: "time"})
	bitMap := make([]int, recLen)
	intVals := make([]int64, recLen)
	floatVals := make([]float64, recLen)
	stringVals := make([]string, recLen)
	boolVals := make([]bool, recLen)
	timeVals := make([]int64, recLen)
	for i := 0; i < recLen; i++ {
		bitMap[i] = 1
		intVals[i] = int64(i * 17)
		floatVals[i] = float64(i) * 133.43
		stringVals[i] = "test"
		boolVals[i] = i%2 == 0
		timeVals[i] = int64(i + 1)
	}
	r = genRowRec(s, bitMap, intVals, bitMap, floatVals, bitMap, stringVals, bitMap, boolVals, timeVals)
	return
}

func BenchmarkRecord_Marshal(b *testing.B) {
	r := generateLargeRecord()
	size := r.CodecSize()
	buf := make([]byte, 0, size)
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf, _ = r.Marshal(buf)
		newO := &record.Record{}
		_ = newO.Unmarshal2(buf)
	}
}
