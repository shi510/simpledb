#ifndef __SIMPLE_DB_H__
#define __SIMPLE_DB_H__
#include <memory>

namespace simpledb{

struct option{
    bool create_if_missing = false;
    bool ordered = false;
    bool binary = false;
    bool delete_previous = false;
};

class db{
public:
    virtual ~db(){ }

    virtual void close() = 0;

    virtual void open(option opt, const std::string name) = 0;

    virtual void get(std::string &val) = 0;

    virtual void get(const std::string key, std::string &val) = 0;

    virtual void put(const std::string key, const std::string val) = 0;

    virtual void move_to_first() = 0;

    virtual bool move_to_next() = 0;

    virtual void erase(const std::string key) = 0;

    virtual size_t num_data() = 0;

protected:
    db(){}
};

std::unique_ptr<db> create_db();

} // end namespace simpledb
#endif // end #ifndef __SIMPLE_DB_H__
