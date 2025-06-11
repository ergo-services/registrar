package etcd

import (
	"encoding/base64"
	"strconv"
	"strings"

	"ergo.services/ergo/lib"
	"ergo.services/ergo/net/edf"
)

// decodeConfigValue processes decoded configuration values with type prefixes
// Supports: "int:123" -> int64(123), "float:3.14" -> float64(3.14), "bool:true" -> bool, "string" -> "string"
func decodeConfigValue(value any) (any, error) {
	// Only process string values for type conversion
	str, ok := value.(string)
	if !ok {
		return value, nil
	}

	// Check for type prefixes
	if strings.HasPrefix(str, "int:") {
		intStr := strings.TrimPrefix(str, "int:")
		intVal, err := strconv.ParseInt(intStr, 10, 64)
		if err != nil {
			return str, nil // Return original string if conversion fails
		}
		return intVal, nil
	}

	if strings.HasPrefix(str, "float:") {
		floatStr := strings.TrimPrefix(str, "float:")
		floatVal, err := strconv.ParseFloat(floatStr, 64)
		if err != nil {
			return str, nil // Return original string if conversion fails
		}
		return floatVal, nil
	}

	if strings.HasPrefix(str, "bool:") {
		boolStr := strings.TrimPrefix(str, "bool:")
		boolVal, err := strconv.ParseBool(boolStr)
		if err != nil {
			return str, nil // Return original string if conversion fails
		}
		return boolVal, nil
	}

	// No prefix, return as string
	return str, nil
}

// encode encodes general data (routes, application routes, etc.)
func encode(value any) (string, error) {
	b := lib.TakeBuffer()
	defer lib.ReleaseBuffer(b)

	if err := edf.Encode(value, b, edf.Options{}); err != nil {
		return "", err
	}

	str := base64.StdEncoding.EncodeToString(b.B)

	return str, nil
}

// decode decodes general data without type conversion
func decode(data []byte) (any, error) {
	b, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return nil, err
	}

	value, _, err := edf.Decode(b, edf.Options{})
	if err != nil {
		return nil, err
	}

	return value, nil
}
