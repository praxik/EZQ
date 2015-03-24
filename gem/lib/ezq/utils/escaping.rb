require 'ezq/utils/common'


module EZQ

  # Un-escapes an escaped string. Cribbed from
  # http://stackoverflow.com/questions/8639642/whats-the-best-way-to-escape-and-unescape-strings
  # Does *not* modify str in place. Returns a new, unescaped string.
  def EZQ.unescape(str)
    str.gsub(/\\(?:([#{UNESCAPES.keys.join}])|u([\da-fA-F]{4}))|\\0?x([\da-fA-F]{2})/) {
      if $1
        if $1 == '\\' then '\\' else UNESCAPES[$1] end
      elsif $2 # escape \u0000 unicode
        ["#$2".hex].pack('U*')
      elsif $3 # escape \0xff or \xff
        [$3].pack('H2')
      end
    }
  end



  UNESCAPES = {'a' => "\x07", 'b' => "\x08", 't' => "\x09",
               'n' => "\x0a", 'v' => "\x0b", 'f' => "\x0c",
               'r' => "\x0d", 'e' => "\x1b", "\\\\" => "\x5c",
               "\"" => "\x22", "'" => "\x27"}



  # Rogue out single backslashes that are not real escape sequences and
  # turn them into double backslashes.
  def EZQ.fix_escapes(text)
    # (?<!\\)  -- no backslashes directly before current match
    # (\\)     -- match a single backslash
    # (?![\\\/\"\'rnbt])  -- not followed by a character that would indicate
    #                        this is already a valid escape sequence:
    #                        backslash, forwardslash, double quote,
    #                        single quote, r, n, b, or t
    # "\\\\\\\\" -- it takes *8* backslashes to indicate two backslashes: one
    #               pass of escaping for the regexp (\\ --> \\\\) and a second
    #               pass of escaping for the ruby string (\\\\ --> \\\\\\\\)
    return text.gsub(/(?<!\\)(\\)(?![\\\/\"\'rnbt])/,"\\\\\\\\")
  end

end
