#pragma once

#include <TableFunctions/ITableFunction.h>
#include <base/types.h>


namespace DB
{

/* numbers(limit), numbers_mt(limit)
 * - the same as SELECT number FROM system.numbers LIMIT limit.
 * Used for testing purposes, as a simple example of table function.
 */
template <bool multithreaded>
class TableFunctionNumbers : public ITableFunction
{
public:
    static constexpr auto name = multithreaded ? "numbers_mt" : "numbers";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }
    
    std::vector<int> generate_series(int start, int stop, int step = 1);
    std::vector<long long> generate_series(long long start, long long stop, long long step = 1);
    std::vector<double> generate_series(double start, double stop, double step = 1.0);

    std::vector<std::chrono::system_clock::time_point> generate_date_array(
        std::chrono::system_clock::time_point start, 
        std::chrono::system_clock::time_point stop, 
        std::chrono::milliseconds step = std::chrono::milliseconds(1));
    
    std::vector<std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>> generate_timestamp_array(
        std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> start, 
        std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> stop, 
        std::chrono::milliseconds step = std::chrono::milliseconds(1));
        
        
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "SystemNumbers"; }

    UInt64 evaluateArgument(ContextPtr context, ASTPtr & argument) const;

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
};


}
