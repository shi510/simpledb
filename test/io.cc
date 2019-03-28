#include "catch2/catch.hpp"
#include "simpledb/core/db.h"

TEST_CASE("r/w verification", "[single-file]"){
    auto rw_test = simpledb::create_db();
    simpledb::option opt;
    std::string hello;

    opt.create_if_missing = true;
    opt.delete_previous = true;

    rw_test->open(opt, "rw_test.simpledb");

    rw_test->put("hello", "world");

    rw_test->get("hello", hello);

    REQUIRE(hello.compare("world") == 0);

    rw_test->close();
}
