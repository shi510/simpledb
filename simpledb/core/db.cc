#include "simpledb/core/db.h"
#include "simpledb/util/util.h"
#include "simpledb/core/io.h"
#include <sstream>
#include <vector>
#include <map>

namespace simpledb{
using namespace util;
using namespace file;
/*
 * TODO:
 * more algorithms need: B-Tree, data cashing, reducing direct calling of IO seek,
 * async algorithm for put, get, delete,
 * data compression to reduce DB size,
 * efficient key-value management,
 * db options, db status, thread-safe,
 * file integrity check.
 */
class simpledb_impl final : public db{
public:
    simpledb_impl();

    ~simpledb_impl();

    void close() override;

    void open(option _opt, const std::string name) override;

    void get(std::string &val) override;

    void get(const std::string key, std::string &val) override;

    void put(const std::string key, const std::string val) override;

    void move_to_first() override;

    bool move_to_next() override;

    void erase(const std::string key) override;

    size_t num_data() override;

protected:
    void init();

    void read_header();

    void write_header();

    void read_disk_info();

    void write_disk_info();

    int header_size();

    uint64_t get_all_item_size();

    std::ios::openmode get_file_mode_by_option();

private:
    /*
     * TODO:
     * Define max item, max size of data, max size of key.
     * dealing with Padding bit, when use int64_t in odd.
     */
    struct header_info{
        uint32_t signature;
        uint32_t version;
        uint64_t number_of_items;
        uint64_t disk_info_offset;
    } hi;

    struct item_disk_info{
        uint64_t data_size;
        uint64_t pos_data_from_first;
    };

    /*
     * TODO:
     * Define max tree-level.
     * Define function to transform from tree to string.
     */
    using disk_pair = std::pair<std::string, item_disk_info *>;
    std::map<std::string, item_disk_info> disk_info_tree;
    std::map<std::string, item_disk_info>::iterator iter;
    std::vector<disk_pair> insertion_order;
    std::vector<disk_pair>::iterator insertion_order_iter;
    int64_t file_size;
    uint64_t last_cursor;
    option _opt;
    file_io file;
};

simpledb_impl::simpledb_impl(){
    init();
}

simpledb_impl::~simpledb_impl(){
    close();
}

void simpledb_impl::close(){
    if(file.is_open()){
        write_header();
        write_disk_info();
        file.close();
        init();
    }
}

void simpledb_impl::open(option opt, const std::string name){
    std::ios::openmode mode;
    _opt = opt;
    mode = get_file_mode_by_option();
    /*
     * TODO:
     * simplify the file open procedure.
     */
    try{
        if(_opt.create_if_missing && !_opt.delete_previous){
            file.create_file(name);
        }
        file.open(name, mode);
    }
    catch(std::string &e){
        throw e;
    }

    file.seek_to_end();
    file_size = file.get_position();
    if(file_size < header_size() && file_size != 0){
        throw std::string("Header size does not match.");
    }

    if(file_size == 0 || _opt.delete_previous){
        write_header();
    }
    else if(!_opt.create_if_missing){
        read_header();
        read_disk_info();
    }
    last_cursor = hi.disk_info_offset;
    file.seek_from_first_to(last_cursor);
}

void simpledb_impl::get(std::string &val){
    item_disk_info target;
    if(_opt.ordered){
        target = iter->second;
    }
    else{
        target = *insertion_order_iter->second;
    }
    val.resize(target.data_size);
    file.seek_from_first_to(target.pos_data_from_first);
    file.read(const_cast<char *>(val.c_str()), val.size());
}

void simpledb_impl::get(const std::string key, std::string &val){
    runtime_assert(disk_info_tree.count(key) == 1, "Key not exists.");
    item_disk_info target= disk_info_tree[key];
    val.resize(target.data_size);
    file.seek_from_first_to(target.pos_data_from_first);
    file.read(const_cast<char *>(val.c_str()), val.size());
}

void simpledb_impl::put(const std::string key, const std::string val){
    runtime_assert(disk_info_tree.count(key) == 0, "Key already exists.");
    item_disk_info new_data;
    new_data.data_size = val.size();
    new_data.pos_data_from_first = last_cursor;
    disk_info_tree[key] = new_data;
    insertion_order.push_back(std::make_pair(key, &disk_info_tree[key]));
    file.write(const_cast<char *>(val.c_str()), val.size());
    last_cursor += new_data.data_size;
}

void simpledb_impl::move_to_first(){
    iter = disk_info_tree.begin();
    insertion_order_iter = insertion_order.begin();
}

bool simpledb_impl::move_to_next(){
    ++iter;
    ++insertion_order_iter;
    if(iter == disk_info_tree.end()){
        return false;
    }
    return true;
}

void simpledb_impl::erase(const std::string key){
    throw std::string("simpledb_impl Delete() "
                      "function dose not supported yet.");
}

size_t simpledb_impl::num_data(){
    return disk_info_tree.size();
}

void simpledb_impl::init(){
    hi.signature = 0x57957295;
    hi.version = 0x00000001;
    hi.number_of_items = 0;
    hi.disk_info_offset = sizeof(header_info);
    disk_info_tree.clear();
    insertion_order.clear();
}

void simpledb_impl::read_header(){
    header_info temp;
    file.seek_to_first();

    file.read(reinterpret_cast<char *>(&temp), sizeof(temp));
    if (temp.signature != hi.signature){
        std::ostringstream info;
        info<<"Signature not matches.";
        info<<"[";
        info<<temp.signature;
        info<<"vs";
        info<<hi.signature;
        info<<"]";
        throw info.str();
    }
    if (temp.version != hi.version){
        std::ostringstream info;
        info << "Version not matches.";
        info << "[";
        info << temp.version << "(opened file)";
        info << "vs";
        info << hi.version << "(current library)";
        info << "]";
        throw info.str();
    }
    hi.number_of_items = temp.number_of_items;
    hi.disk_info_offset = temp.disk_info_offset;
}

void simpledb_impl::write_header(){
    file.seek_to_first();
    hi.version = hi.version;
    hi.signature = hi.signature;
    hi.number_of_items = disk_info_tree.size();
    hi.disk_info_offset = header_size() + get_all_item_size();
    file.write(reinterpret_cast<char *>(&hi), sizeof(header_info));
}

void simpledb_impl::read_disk_info(){
    std::vector<item_disk_info> vec_disk_info;
    std::vector<char> keys;
    uint64_t total_key_size = 0;
    uint64_t accum_key_size = 0;
    uint64_t key_count = 0;

    vec_disk_info.resize(hi.number_of_items);
    file.seek_from_first_to(hi.disk_info_offset);
    file.read(reinterpret_cast<char *>(vec_disk_info.data()),
              sizeof(item_disk_info) * hi.number_of_items
              );

    total_key_size = file_size - (file.get_position());
    keys.resize(total_key_size);
    file.read(keys.data(), total_key_size);

    key_count = file.get_count_of_read();
    runtime_assert(key_count == total_key_size,
                   "the number of data and the number of key "
                   "are not same. (maybe, file has been corrupted.)"
                   );
    for(int n = 0; n < hi.number_of_items; ++n){
        std::string key(keys.data() + accum_key_size);
        auto &di = vec_disk_info[n];
        di.data_size = di.data_size;
        di.pos_data_from_first = di.pos_data_from_first;
        disk_info_tree[key] = di;
        insertion_order.push_back(std::make_pair(key, &disk_info_tree[key]));
        accum_key_size += key.size() + 1;
    }
    file.seek_from_first_to(header_size() + get_all_item_size());
}

void simpledb_impl::write_disk_info(){
    file.seek_from_first_to(hi.disk_info_offset);

    /*
     * TODO:
     * 1. Merge item_disk_info and key into one string for one write op.
     * maybe it also needs the algorithm that not use for-loop.
     * 2. remove the conditional branch.
     */
    if(_opt.ordered){
        for(auto &iter : disk_info_tree){
            iter.second.data_size = iter.second.data_size;
            iter.second.pos_data_from_first = iter.second.pos_data_from_first;
            file.write(reinterpret_cast<char *>(&iter.second), sizeof(item_disk_info));
        }

        for(auto &iter : disk_info_tree){
            file.write(const_cast<char *>(iter.first.c_str()), iter.first.size() + 1);
        }
    }
    else{
        for(auto &iter : insertion_order){
            iter.second->data_size = iter.second->data_size;
            iter.second->pos_data_from_first = iter.second->pos_data_from_first;
            file.write(reinterpret_cast<char *>(iter.second), sizeof(item_disk_info));
        }

        for(auto &iter : insertion_order){
            file.write(const_cast<char *>(iter.first.c_str()), iter.first.size() + 1);
        }
    }
}

int simpledb_impl::header_size(){
    return sizeof(header_info);
}

uint64_t simpledb_impl::get_all_item_size(){
    uint64_t size = 0;
    /*
     * TODO:
     * implement the faster method than the accumulation one by for-loop.
     */
    for(auto &item : disk_info_tree){
        size += item.second.data_size;
    }
    return size;
}

std::ios::openmode simpledb_impl::get_file_mode_by_option(){
    std::ios::openmode mode;
    mode = std::ios::in | std::ios::out;
    mode = _opt.binary ? mode | std::ios::binary : mode;
    mode = _opt.delete_previous ? mode | std::ios::trunc : mode;
    return mode;
}

std::unique_ptr<db> create_db(){
    simpledb_impl *sdb = new simpledb_impl;
    return std::unique_ptr<simpledb_impl>(sdb);
}

} // end namespace simpledb
