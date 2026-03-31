package dbi

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// set represents a single set_config statement and its corresponding value.
type set struct {
	SQL   string
	Value string
}

// SetConfig holds a set of session-local configuration parameters to be applied
// using set_config within a transaction.
type SetConfig struct {
	sets [][2]string
}

// NewSetConfig returns a new SetConfig
func NewSetConfig(tuples ...string) (*SetConfig, error) {
	sc := &SetConfig{}

	if len(tuples)%2 != 0 {
		return nil, errors.New("uneven tuples")
	}

	for i := 0; i < len(tuples); i += 2 {
		if err := sc.Add(tuples[i], tuples[i+1]); err != nil {
			return nil, err
		}
	}

	return sc, nil
}

// NewSetConfigFromMap returns a new SetConfig
func NewSetConfigFromMap(m map[string]string) (*SetConfig, error) {
	sc := &SetConfig{}

	for k, v := range m {
		if err := sc.Add(k, v); err != nil {
			return nil, err
		}
	}

	return sc, nil
}

// Add adds or replaces a configuration parameter.
//
// name must be lower case, include a namespace (e.g. "app.key"), contain no
// whitespace, and be at most 63 characters long. If name already exists, its
// value is replaced.
func (s *SetConfig) Add(name, value string) error {
	if len(name) > 63 {
		return errors.New("name too long")
	}

	namespace, _, ok := strings.Cut(name, ".")
	if !ok {
		return errors.New("name has no namespace")
	}
	if len(namespace) < 1 {
		return errors.New("namespace too short")
	}

	if name != strings.ToLower(name) {
		return errors.New("name must be lower case")
	}

	for _, r := range name {
		switch {
		case unicode.IsSpace(r):
			return errors.New("name must not contain whitespace")
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '_', r == '.':
			// ok
		default:
			return fmt.Errorf("invalid character: %q", r)
		}
	}

	found := false
	for i, set := range s.sets {
		if set[0] == name {
			s.sets[i][1] = value
			found = true
			break
		}
	}

	if !found {
		s.sets = append(s.sets, [2]string{name, value})
	}

	return nil
}

// Delete removes the configuration parameter with the given name.
// It returns true if the parameter was found and removed.
func (s *SetConfig) Delete(name string) bool {
	for i, set := range s.sets {
		if set[0] == name {
			s.sets = append(s.sets[:i], s.sets[i+1:]...)
			return true
		}
	}

	return false
}

// String returns a human-readable representation of the stored parameters.
func (s *SetConfig) String() string {
	var sb strings.Builder
	for _, set := range s.sets {
		sb.WriteString(set[0] + " = " + set[1] + "\n")
	}

	return sb.String()
}

// queries returns the set_config statements for the stored parameters.
//
// Each parameter is converted to "set local <name> = $1" with the value
// provided separately for execution.
func (s *SetConfig) queries() []*set {
	var ret []*set

	for _, e := range s.sets {
		ret = append(ret, &set{SQL: fmt.Sprintf("select set_config('%s', $1, true)", e[0]), Value: e[1]})
	}

	return ret
}
