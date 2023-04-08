#include <iostream>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace Conv
{

    class FunctionConv : public IFunction
    {
    public:
        static constexpr auto name = "conv";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionConv>(); }

        String getName() const override { return name; }

        bool useDefaultImplementationForNulls() const override { return false; }

        bool useDefaultImplementationForNothing() const override { return false; }

        bool useDefaultImplementationForConstants() const override { return true; }

        size_t getNumberOfArguments() const override { return 1; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeString>(); }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            const auto & elem = arguments[0];
            std::cout << "working? " << input_rows_count << ", " << elem.name << std::endl;
            // return ColumnUInt8::create(input_rows_count, isColumnNullable(*elem.column) || elem.type->isLowCardinalityNullable());
            return ColumnString::create();
        }
    };
}

REGISTER_FUNCTION(Conv)
{
    factory.registerFunction<Conv::FunctionConv>();
}

}
