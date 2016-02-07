namespace java edu.uchicago.mpcs53013.crimeSummary

struct CrimeSummary {
	1: required byte month;
	2: required byte day;
	3: required i16 year;
	4: required byte hour;
	5: required byte minute;
	6: required string ap;
	7: required string block;
	8: required string type;
	9: required i16 ward;
}

