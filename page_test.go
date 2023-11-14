package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"testing"
)

func TestPage_SetKeyOffset(t *testing.T) {
	type fields struct {
		Data []byte
	}
	type args struct {
		slot   uint32
		offset uint32
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   uint32
	}{
		{
			name: "set offset to first slot",
			fields: fields{
				Data: []byte{0, 0, 0, 0, 0, 0},
			},
			args: args{
				slot:   1,
				offset: 64,
			},
			want: 64,
		},
		{
			name: "set offset to second slot",
			fields: fields{
				Data: []byte{
					0, 0, 0, 0, 0, 0, // first slot
					0, 0, 0, 0, 0, 0, // second slot
				},
			},
			args: args{
				slot:   2,
				offset: 64,
			},
			want: 64,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Page{
				Data: tt.fields.Data,
			}
			p.SetKeyOffset(tt.args.slot, tt.args.offset)
			var off uint32
			_ = binary.Read(bytes.NewBuffer(p.slotBytes(tt.args.slot)), binary.LittleEndian, &off)
			if off != tt.want {
				t.Errorf("Page.SetKeyOffset() = %v, want %v", off, tt.want)
			}
		})
	}
}

func TestPage_SetKey(t *testing.T) {
	type fields struct {
		Data []byte
	}
	type args struct {
		bytes []byte
		slot  uint32
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		{
			name: "set key of first slot",
			fields: fields{
				Data: []byte{
					15, 0, 0, 0, 0, 0, // first slot
					12, 0, 0, 0, 0, 0, // second slot
					3, 0, 0, 0, // key 2(keyLen + keyValue)
					4, 0, 0, 0, 0, // key 1(keyLen + keyValue)
				},
			},
			args: args{
				bytes: []byte{1, 2, 3, 4},
				slot:  1,
			},
			want: []byte{4, 1, 2, 3, 4},
		},
		{
			name: "set key of second slot",
			fields: fields{
				Data: []byte{
					15, 0, 0, 0, 0, 0, // first slot
					12, 0, 0, 0, 0, 0, // second slot
					3, 0, 0, 0, // key 2(keyLen + keyValue)
					4, 0, 0, 0, 0, // key 1(keyLen + keyValue)
				},
			},
			args: args{
				bytes: []byte{5, 6, 7},
				slot:  2,
			},
			want: []byte{3, 5, 6, 7},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Page{
				Data: tt.fields.Data,
			}
			p.SetKey(tt.args.bytes, tt.args.slot)
			off := p.KeyOffset(tt.args.slot)
			keyLen := uint32(p.Data[off])
			if !bytes.Equal(p.Data[off:off+1+keyLen], tt.want) {
				t.Errorf("Page.SetKey() = %v, want %v", p.Data[off:off+4], tt.args.bytes)
			}
		})
	}
}

func TestPage_SetValue(t *testing.T) {
	type fields struct {
		Data []byte
	}
	type args struct {
		bytes []byte
		slot  uint32
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		{
			name: "set value of first slot",
			fields: fields{
				Data: []byte{
					23, 0, 0, 0, 0, 0, // first slot
					12, 0, 0, 0, 0, 0, // second slot
					3, 0, 0, 0, // key 2(keyLen + keyValue)
					6, 0, 0, 0, 0, 0, 0, // value 2(valueLen + value)
					4, 0, 0, 0, 0, // key 1(keyLen + keyValue)
					6, 0, 0, 0, 0, 0, 0, // value 1(valueLen + value)
				},
			},
			args: args{
				bytes: []byte{1, 2, 3, 4, 5, 6},
				slot:  1,
			},
			want: []byte{6, 1, 2, 3, 4, 5, 6},
		},
		{
			name: "set value of second slot",
			fields: fields{
				Data: []byte{
					23, 0, 0, 0, 0, 0, // first slot
					12, 0, 0, 0, 0, 0, // second slot
					3, 0, 0, 0, // key 2(keyLen + keyValue)
					6, 0, 0, 0, 0, 0, 0, // value 2(valueLen + value)
					4, 0, 0, 0, 0, // key 1(keyLen + keyValue)
					6, 0, 0, 0, 0, 0, 0, // value 1(valueLen + value)
				},
			},
			args: args{
				bytes: []byte{7, 8, 9, 10, 11, 12},
				slot:  2,
			},
			want: []byte{6, 7, 8, 9, 10, 11, 12},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Page{
				Data: tt.fields.Data,
			}
			p.SetValue(tt.args.bytes, tt.args.slot)

			off := p.ValueOffset(tt.args.slot)
			valLen := uint32(p.Data[off])
			if !bytes.Equal(p.Data[off:off+1+valLen], tt.want) {
				t.Errorf("Page.SetValue() = %v, want %v", p.Data[off:off+4], tt.args.bytes)
			}
		})
	}
}

func TestPutID(t *testing.T) {
	type args struct {
		dest [BtId]uint8
		id   uid
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "put id",
			args: args{
				dest: [BtId]uint8{},
				id:   2,
			},
			want: []byte{0, 0, 0, 0, 0, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			PutID(&tt.args.dest, tt.args.id)
			if !bytes.Equal(tt.args.dest[:], tt.want) {
				t.Errorf("PutID() = %v, want %v", tt.args.dest[:], tt.want)
			}
		})
	}
}

func TestGetID(t *testing.T) {
	type args struct {
		src *[BtId]uint8
	}
	tests := []struct {
		name string
		args args
		want uid
	}{
		{
			name: "get id",
			args: args{
				src: &[BtId]uint8{0, 0, 0, 0, 1, 2},
			},
			want: 258,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetID(tt.args.src); got != tt.want {
				t.Errorf("GetID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPage_FindSlot(t *testing.T) {
	data := []byte{
		40, 0, 0, 0, 0, 0, // first slot
		29, 0, 0, 0, 0, 0, // second slot
		18, 0, 0, 0, 0, 0, // third slot
		3, 2, 1, 0, // key 3(keyLen + keyValue)
		6, 0, 0, 0, 0, 0, 0, // value 3(valueLen + value)
		3, 0, 1, 0, // key 2(keyLen + keyValue)
		6, 0, 0, 0, 0, 0, 0, // value 2(valueLen + value)
		4, 0, 0, 1, 1, // key 1(keyLen + keyValue)
		6, 0, 0, 0, 0, 0, 0, // value 1(valueLen + value)
	}
	type args struct {
		key []byte
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "find first slot",
			args: args{
				key: []byte{0, 0, 1},
			},
			want: 1,
		},
		{
			name: "find first slot by smaller key",
			args: args{
				key: []byte{0, 0, 0, 1},
			},
			want: 1,
		},
		{
			name: "find second slot",
			args: args{
				key: []byte{0, 1, 0},
			},
			want: 2,
		},
		{
			name: "find third slot",
			args: args{
				key: []byte{2, 1, 0},
			},
			want: 3,
		},
		{
			name: "find third slot by bigger key",
			args: args{
				key: []byte{2, 1, 1},
			},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Page{
				PageHeader: PageHeader{
					Cnt: 3,
				},
				Data: data,
			}
			if got := p.FindSlot(tt.args.key); got != tt.want {
				t.Errorf("FindSlot() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCopyPage(t *testing.T) {
	set1 := PageSet{
		page:  NewPage(10),
		latch: &LatchSet{},
	}

	set2 := PageSet{
		page: NewPage(10),
	}

	if set1.page.Free {
		t.Errorf("set1.page.Free = %v, want %v", set1.page.Free, false)
	}

	if bytes.Compare(set1.page.Data, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) != 0 {
		t.Errorf("set1.page.Data = %v, want %v", set1.page.Data, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	}

	MemCpyPage(set2.page, set1.page)
	set1.page.Free = true
	set1.page.Data[0] = 1
	if set2.page.Free {
		t.Errorf("set2.page.Free = %v, want %v", set2.page.Free, false)
	}
	if bytes.Compare(set2.page.Data, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) != 0 {
		t.Errorf("set2.page.Data = %v, want %v", set2.page.Data, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	}
}
func TestCheckPage(t *testing.T) {
	f, err := os.OpenFile("data/page.db", os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}

	// ファイルの数値をすべてバイト配列に読み込む
	// ファイルの内容はすべて文字列で、空白文字で区切られている
	var data []byte
	for {
		var b byte
		_, err := fmt.Fscanf(f, "%d", &b)
		if err != nil {
			break
		}
		data = append(data, b)
	}
	fmt.Printf("size: %v\n", len(data))

	// バイト配列を先頭から以下のレイアウトで解析する
	// レイアウトは以下の通り
	// 1. 4byte: Offset
	// 2. 1byte: SlotType
	// 3. 1byte: Dead
	// Offsetはこのファイルの先頭からのオフセットを表すので、バイト配列のサイズを超える場合は問題があるので、検出すべき
	// また、ファイルの後半は上記のレイアウトとは異なるデータを入れる領域となるため、Offset値が上記のレイアウトのデータと重複した時点で検出すべき
	reader := bytes.NewReader(data)
	slotNum := 0
	for {
		var offsetBytes [4]byte
		var typ SlotType
		var dead bool
		if _, err := reader.Read(offsetBytes[:]); err != nil {
			log.Panicf("invalid offset: %v", err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &typ); err != nil {
			log.Panicf("invalid type: %v", err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &dead); err != nil {
			log.Panicf("invalid dead: %v", err)
		}
		slotNum++
		fmt.Printf("slot: %v, offset: %v, type: %v, dead: %v\n", slotNum, offsetBytes, typ, dead)
		var offset uint32
		_ = binary.Read(bytes.NewBuffer(offsetBytes[:]), binary.LittleEndian, &offset)
		if offset > uint32(len(data)) {
			log.Panicf("invalid offset: %v", offset)
		}
	}

}
