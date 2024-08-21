use crate::pattern::Pattern;
use uuid::Uuid;

pub struct PatternRegistry {
    patterns: Vec<Pattern>,
}

impl PatternRegistry {
    pub const fn new() -> PatternRegistry {
        PatternRegistry { patterns: Vec::new() }
    }

    pub fn find_pattern(&mut self, str_text: &Vec<String>, i_text: &Vec<u64>, sample: String) -> &Pattern {
        let mut idx: i32 = -1;
        let mut mtc = 0;
        for i in 0..self.patterns.len() {
            mtc = self.patterns[i].match_text(&i_text);
            if mtc == -1 || mtc > self.patterns[i].fluct {
                continue;
            }
            idx = i as i32;
            break;
        }

        if idx == -1 {
            let pattern = Pattern::new(Uuid::new_v4().to_string(), &i_text, &str_text, sample);
            self.patterns.push(pattern);
            idx = (self.patterns.len() - 1) as i32;
        } else if mtc != 0 {
            self.patterns[idx as usize].adjust_pattern(&i_text);
        }
        return &self.patterns[idx as usize];
    }

    pub fn to_string(&self) -> String {
        let mut s = String::new();
        for i in 0..self.patterns.len() {
            s += self.patterns[i].to_string().as_str();
            s += "\n";
        }
        return s
    }
}

pub static mut REGISTRY: PatternRegistry = PatternRegistry::new();