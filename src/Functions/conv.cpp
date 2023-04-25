#include <iostream>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/Base58.h>
#include <Functions/BaseConversion.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
}

namespace Conv
{

    class FunctionConv : public IFunction
    {
    public:
        static constexpr auto name = "conv";

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionConv>(); }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 3; }

        bool useDefaultImplementationForConstants() const override { return true; }

        bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            WhichDataType which(arguments[0]);

            if (!which.isStringOrFixedString())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

            return std::make_shared<DataTypeString>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            const ColumnPtr column_string = arguments[0].column;
            const ColumnString * input = checkAndGetColumn<ColumnString>(column_string.get());

            const ColumnPtr column_from_base = arguments[1].column;
            const ColumnVector<UInt8> * from_base = checkAndGetColumn<ColumnVector<UInt8>>(column_from_base.get());

            const ColumnPtr column_to_base = arguments[2].column;
            const ColumnVector<UInt8> * to_base = checkAndGetColumn<ColumnVector<UInt8>>(column_to_base.get());

            if (!input)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first argument of function {}, must be String",
                    arguments[0].column->getName(),
                    getName());

            if (!from_base)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of second argument of function {}, must be UInt8",
                    arguments[1].column->getName(),
                    getName());

            if (!to_base)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of third argument of function {}, must be UInt8",
                    arguments[2].column->getName(),
                    getName());

            auto dst_column = ColumnString::create();

            process(*input, *from_base, *to_base, dst_column, input_rows_count);

            return dst_column;
        }

    private:
        static void process(
            const ColumnString & src_column,
            const ColumnUInt8 & from_base,
            const ColumnUInt8 & to_base,
            ColumnString::MutablePtr & dst_column,
            size_t input_rows_count)
        {
            auto & dst_data = dst_column->getChars();
            auto & dst_offsets = dst_column->getOffsets();

            auto & from_base_data = from_base.getData();
            auto & to_base_data = to_base.getData();

            size_t max_result_size = static_cast<size_t>(ceil(2 * src_column.getChars().size() + 1));

            dst_data.resize(max_result_size);
            dst_offsets.resize(input_rows_count);

            const ColumnString::Offsets & src_offsets = src_column.getOffsets();

            const auto * src = src_column.getChars().data();
            auto * dst = dst_data.data();

            size_t prev_src_offset = 0;
            size_t current_dst_offset = 0;

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                size_t current_src_offset = src_offsets[row];
                size_t src_length = current_src_offset - prev_src_offset - 1;

                if (from_base_data[row] < 2 || from_base_data[row] > 36 || to_base_data[row] < 2 || to_base_data[row] > 36)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal base for conversion");

                size_t encoded_size = BaseConversion::conv(
                    &src[prev_src_offset], src_length, &dst[current_dst_offset], from_base_data[row], to_base_data[row]);

                prev_src_offset = current_src_offset;
                current_dst_offset += encoded_size;
                dst[current_dst_offset] = 0;
                ++current_dst_offset;

                dst_offsets[row] = current_dst_offset;
            }

            dst_data.resize(current_dst_offset);
        }
    };
}

REGISTER_FUNCTION(Conv)
{
    factory.registerFunction<Conv::FunctionConv>();
}
}
