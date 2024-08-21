use regex::{Regex, CaptureMatches, Match};

/*pub fn tokenize(re: &Regex, text: &str) -> CaptureMatches {
    return re.captures_iter(text);
}*/

pub struct Tokenizer<'a> {
    text: String,
    pos: usize,
    re: Regex,
    iter: Option<CaptureMatches<'a, 'a>>
}

impl Tokenizer<'_> {
    pub fn new<'a>(text: &'a str) -> Tokenizer<'a> {
        let mut res = Tokenizer {
            text: text.to_string(),
            pos: 0,
            re: Regex::new(r"([\p{L}_]+|[\d.]+|[^\p{L}_\d.]+)\s*").unwrap(),
            iter: None
        };
        res
    }
}

impl Iterator for Tokenizer<'_> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        None
        /*let cap: Option<Match> = None;
        if let Some(c) = cap {
            self.pos += c.get(0).unwrap().end();
            Some(c.get(0).unwrap().as_str().to_string())
        } else {
            None
        }*/
    }
}

#[test]
fn test_tokenizer() {
    let text = "Hello, world! 123";
    let mut tokenizer = Tokenizer::new(text);
}