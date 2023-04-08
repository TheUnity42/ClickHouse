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

        size_t getNumberOfArguments() const override { return 3; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeString>(); }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            const ColumnWithTypeAndName & arg_from = arguments[0];
            const ColumnWithTypeAndName & arg_base_from = arguments[1];
            const ColumnWithTypeAndName & arg_base_to = arguments[2];

            std::cout << "working? " << input_rows_count << std::endl;
            std::cout << "arg_from: " << arg_from.column->getName() << std::endl;
            std::cout << "arg_base_from: " << arg_base_from.column->getName() << std::endl;
            std::cout << "arg_base_to: " << arg_base_to.column->getName() << std::endl;
            
            // return ColumnUInt8::create(input_rows_count, isColumnNullable(*elem.column) || elem.type->isLowCardinalityNullable());
            auto result = ColumnString::create();

            return result;
        }
    };
}

REGISTER_FUNCTION(Conv)
{
    factory.registerFunction<Conv::FunctionConv>();
}

}
