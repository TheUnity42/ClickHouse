#include <Core/Types.h>
#include <Functions/BaseConversion.h>

namespace DB
{

int BaseConversion::valueOf(UInt8 c)
{
    if (c >= '0' && c <= '9')
        return static_cast<int>(c) - '0';
    else
        return static_cast<int>(c) - 'A' + 10;
}

UInt8 BaseConversion::charOf(int n)
{
    if (n >= 0 && n <= 9)
        return static_cast<UInt8>(n + '0');
    else
        return static_cast<UInt8>(n - 10 + 'A');
}

int BaseConversion::toDecimal(const UInt8 * s, size_t length, UInt8 base)
{
    // Initialize base
    int power = 1;
    int num = 0;


    for (size_t i = length - 1; static_cast<long>(i) >= 0; i--)
    {
        if (valueOf(s[i]) >= base)
        {
            printf("Invalid Number");
            return -1;
        }

        // Update num
        num += valueOf(s[i]) * power;

        // Update power
        power = power * base;
    }

    return num;
}

size_t BaseConversion::fromDecimal(int n, int base, UInt8 * dst)
{
    // Initialize result
    size_t length = 0;

    // Convert input number is given base by repeatedly
    // dividing it by base and taking remainder
    while (n > 0)
    {
        dst[length++] = charOf(n % base);
        n /= base;
    }

    // Reverse the result
    UInt8 * start = dst;
    UInt8 * end = dst + length - 1;
    while (start < end)
    {
        UInt8 temp = *start;
        *start = *end;
        *end = temp;
        start++;
        end--;
    }

    return length;
}

size_t BaseConversion::conv(const UInt8 * src, size_t length, UInt8 * dst, int base1, int base2)
{
    int num = toDecimal(src, length, base1);
    return fromDecimal(num, base2, dst);
}
}
