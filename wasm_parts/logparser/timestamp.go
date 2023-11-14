package logparser

const (
	lookForTimestampLimit = 100
)

func containsTimestamp(line string) bool {
	if len(line) > lookForTimestampLimit {
		line = line[:lookForTimestampLimit]
	}
	var digits, colons int
	for _, r := range line {
		switch {
		case r >= '0' && r <= '9':
			digits++
			if digits > 2 {
				digits = 0
			}
			if digits == 2 && colons == 2 {
				return true
			}
		case r == ':':
			if digits == 2 {
				colons++
			}
			digits = 0
		default:
			digits, colons = 0, 0
		}
	}
	return false
}
