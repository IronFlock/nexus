package serialize

import (
	"encoding/base64"
	"errors"
	"reflect"

	"github.com/ugorji/go/codec"

	"github.com/ironflock/nexus/v3/wamp"
)

var jh *codec.JsonHandle //nolint:gochecknoglobals

func init() {
	jh = &codec.JsonHandle{}
	jh.MapType = reflect.TypeFor[map[string]any]()
}

// JSONSerializer is an implementation of Serializer that handles
// serializing and deserializing json encoded payloads.
type JSONSerializer struct{}

// ID returns the JSON Serialization constant.
func (s *JSONSerializer) ID() Serialization { return JSON }

// Serialize encodes a Message into a json payload.
//
// Recognizes *wamp.SharedMessage and returns the pre-encoded bytes
// from its cache when present — eliminates duplicate JSON encoding
// across high-fan-out broker dispatches.
func (s *JSONSerializer) Serialize(msg wamp.Message) ([]byte, error) {
	if shared, ok := msg.(*wamp.SharedMessage); ok {
		if b, hit := shared.Cached(int(JSON)); hit {
			return b, nil
		}
		// Fallback for the rare case the broker didn't pre-encode for
		// this format — encode the inner message directly.
		var b []byte
		err := codec.NewEncoderBytes(&b, jh).Encode(msgToList(shared.Inner))
		return b, err
	}
	var b []byte
	err := codec.NewEncoderBytes(&b, jh).Encode(msgToList(msg))
	return b, err
}

// Deserialize decodes a json payload into a Message.
func (s *JSONSerializer) Deserialize(data []byte) (wamp.Message, error) {
	var v []any
	err := codec.NewDecoderBytes(data, jh).Decode(&v)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, errors.New("invalid message")
	}

	// json deserializer gives us an uint64 instead of an int64, whyever it
	// doesn't matter here, because valid values are only within an 8bit range.
	utyp, ok := v[0].(uint64)
	if !ok {
		return nil, errors.New("unsupported message format")
	}
	typ := int(utyp) //nolint:gosec
	return listToMsg(wamp.MessageType(typ), v)
}

// SerializeDataItem encodes any object/structure into a json payload.
func (s *JSONSerializer) SerializeDataItem(item any) ([]byte, error) {
	var b []byte
	err := codec.NewEncoderBytes(&b, jh).Encode(item)
	return b, err
}

// DeserializeDataItem decodes a json payload into an object/structure.
func (s *JSONSerializer) DeserializeDataItem(data []byte, v any) error {
	return codec.NewDecoderBytes(data, jh).Decode(&v)
}

// Binary data follows a convention for conversion to JSON strings.
//
// A byte array is converted to a JSON string as follows:
//
// 1. convert the byte array to a Base64 encoded (host language) string
// 2. prepend the string with a \0 character
// 3. serialize the string to a JSON string
type BinaryData []byte

func (b BinaryData) MarshalJSON() ([]byte, error) {
	s := base64.StdEncoding.EncodeToString([]byte(b))
	var out []byte
	return out, codec.NewEncoderBytes(&out, jh).Encode("\x00" + s)
}

func (b *BinaryData) UnmarshalJSON(v []byte) error {
	var s string
	err := codec.NewDecoderBytes(v, jh).Decode(&s)
	if err != nil {
		return err
	}
	if s[0] != '\x00' {
		return errors.New("binary string does not start with NUL")
	}
	*b, err = base64.StdEncoding.DecodeString(s[1:])
	return err
}
