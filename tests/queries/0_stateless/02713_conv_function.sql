-- Coverage for regular conversions
SELECT conv('8',10,2);
SELECT conv('8',10,10);
SELECT conv('1011',2,10);
SELECT conv('A',16,10);
SELECT conv('A',16,2);
SELECT conv('Z',36,10);
SELECT conv('ZZZ',36,10);

-- Test Errors
SELECT conv(10,10,10); -- { serverError 43 }
SELECT conv('10','10',2); -- { serverError 44 }
SELECT conv(10); -- { serverError 42 }

-- Edge Errors
SELECT conv('10',10,1); -- { serverError 44 }
SELECT conv('10',10,2);
SELECT conv('10',10,37); -- { serverError 44 }
SELECT conv('10',10,36);
SELECT conv('10',1,10); -- { serverError 44 }
SELECT conv('10',2,10);
SELECT conv('10',37,10); -- { serverError 44 }
SELECT conv('10',36,10);