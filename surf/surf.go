package surf

import (
	"bytes"
	"io"
)

type SuRF struct {
	ld loudsDense
	ls loudsSparse
}

// Get returns the values mapped by the key, may return value for keys doesn't in SuRF.
func (s *SuRF) Get(key []byte) ([]byte, bool) {
	cont, depth, value, ok := s.ld.Get(key)
	if !ok || cont < 0 {
		return value, ok
	}
	return s.ls.Get(key, depth, uint32(cont))
}

// MarshalSize returns the size of SuRF after serialization.
func (s *SuRF) MarshalSize() int64 {
	return s.ld.MarshalSize() + s.ls.MarshalSize() + s.ld.values.MarshalSize() + s.ls.values.MarshalSize()
}

// Marshal returns the serialized SuRF.
func (s *SuRF) Marshal() []byte {
	w := bytes.NewBuffer(make([]byte, 0, s.MarshalSize()))
	_ = s.WriteTo(w)
	return w.Bytes()
}

// WriteTo serialize SuRF to writer.
func (s *SuRF) WriteTo(w io.Writer) error {
	if err := s.ld.WriteTo(w); err != nil {
		return err
	}
	if err := s.ls.WriteTo(w); err != nil {
		return err
	}
	if err := s.ld.values.WriteTo(w); err != nil {
		return err
	}
	if err := s.ls.values.WriteTo(w); err != nil {
		return err
	}
	return nil
}

// Unmarshal deserialize SuRF from bytes.
func (s *SuRF) Unmarshal(b []byte) {
	b = s.ld.Unmarshal(b)
	b = s.ls.Unmarshal(b)
	b = s.ld.values.Unmarshal(b)
	s.ls.values.Unmarshal(b)
}
