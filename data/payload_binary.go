/**
 * Created by Andrey Gayvoronsky on 23/08/16.
 * (C) Luxms-BI
 */

package data

type Binary struct {
	data []byte
}

func NewBinaryPayload(d []byte)(*Binary) {
	return &Binary {data: d}
}

func NewTextPayload(s string)(*Binary) {
	return &Binary {data: []byte(s)}
}

func (p *Binary) ClonePayload()(Payload) {
	dc := make([]byte, len(p.data))
	copy(dc, p.data)
	return &Binary {data: dc}
}

func (p *Binary)MarshalPayload() []byte {
	return p.data
}

func (p *Binary)UnmarshalPayload(v interface{}) (error) {
	return nil
}



