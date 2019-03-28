#include "catch2/catch.hpp"
#include "simpledb/core/db.h"
#include <random>
#include <map>
#include <iostream>

TEST_CASE("try 6GB size with coarse-grained data", "[large-file]"){
    return;
    const uint64_t size_3gb = (1ul<<30) * 6 / sizeof(int);
    auto large_test = simpledb::create_db();
    simpledb::option opt;
    std::mt19937 rng;
    std::uniform_int_distribution<int> uniform(1<<15, 1<<20);
    std::string ret;
    std::map<std::string, int> key_size;

    opt.create_if_missing = true;
    opt.delete_previous = true;

    large_test->open(opt, "large_rw_test.simpledb");

    unsigned int written = 0;
    int id = 0;
    while(1){
        int increment = 0;
        std::vector<int> vec(uniform(rng));
        std::string data;
        std::generate(vec.begin(), vec.end(), [&increment](){
            return ++increment;
        });

        data.assign((char *)vec.data(), vec.size() * sizeof(int));
        large_test->put(std::to_string(id), data);

        written += vec.size();
        key_size[std::to_string(id)] = vec.size() * sizeof(int);
        if(written >= size_3gb){
            break;
        }
        ++id;
    }
    large_test->close();

    opt.create_if_missing = false;
    opt.delete_previous = false;
    large_test->open(opt, "large_rw_test.simpledb");

    for(auto pair : key_size){

        std::vector<int> decoded;
        std::string data;
        unsigned long accum;
        unsigned long answer;
        large_test->get(pair.first, data);

        REQUIRE(data.size() == pair.second);
        decoded = std::vector<int>((int *)data.data(),
                                   (int *)data.data() + data.size() / 4);
        accum = 0;
        for(int n = 0; n < decoded.size(); ++n){
            accum += decoded[n];
        }
        answer = decoded[0] + decoded[decoded.size() - 1];
        answer = answer * (decoded.size() / 2);
        answer = decoded.size() % 2 ?
                    answer + decoded[decoded.size() / 2] : answer;
        REQUIRE(accum == answer);
    }

    large_test->close();
}

TEST_CASE("try 6GB size with fine-grained data", "[large-file]"){
    return;
    const uint64_t size_3gb = (1ul<<30) * 6 / sizeof(int);
    auto large_test = simpledb::create_db();
    simpledb::option opt;
    std::mt19937 rng;
    std::uniform_int_distribution<int> uniform(1, 1<<10);
    std::string ret;
    std::map<std::string, int> key_size;

    opt.create_if_missing = true;
    opt.delete_previous = true;

    large_test->open(opt, "large_rw_test.simpledb");

    unsigned int written = 0;
    int id = 0;
    while(1){
        int increment = 0;
        std::vector<int> vec(uniform(rng));
        std::string data;
        std::generate(vec.begin(), vec.end(), [&increment](){
            return ++increment;
        });

        data.assign((char *)vec.data(), vec.size() * sizeof(int));
        large_test->put(std::to_string(id), data);

        written += vec.size();
        key_size[std::to_string(id)] = vec.size() * sizeof(int);
        if(written >= size_3gb){
            break;
        }
        ++id;
    }

    large_test->close();

    opt.create_if_missing = false;
    opt.delete_previous = false;
    large_test->open(opt, "large_rw_test.simpledb");

    for(auto pair : key_size){
        std::vector<int> decoded;
        std::string data;
        unsigned long accum;
        unsigned long answer;
        large_test->get(pair.first, data);

        REQUIRE(data.size() == pair.second);
        decoded = std::vector<int>((int *)data.data(),
                                   (int *)data.data() + data.size() / 4);
        accum = 0;
        for(int n = 0; n < decoded.size(); ++n){
            accum += decoded[n];
        }
        answer = decoded[0] + decoded[decoded.size() - 1];
        answer = answer * (decoded.size() / 2);
        answer = decoded.size() % 2 ?
        answer + decoded[decoded.size() / 2] : answer;
        REQUIRE(accum == answer);
    }

    large_test->close();
}
