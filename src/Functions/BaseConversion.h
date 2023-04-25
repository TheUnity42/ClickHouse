#pragma once

namespace DB {
    class BaseConversion {
        public:
            int static valueOf(UInt8 c);
            UInt8 static charOf(int n);
            int static toDecimal(const UInt8 * s, size_t length, UInt8 base);
            static size_t fromDecimal(int n, int base, UInt8 * dst);
            static size_t conv(const UInt8 * src, size_t length, UInt8 * dst, int base1, int base2);
    };
}
