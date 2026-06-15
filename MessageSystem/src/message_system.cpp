#include "message_system.h"

#include "utils/hash.h"
#include "tipc/tipcc.h"


MessageSystem::MessageSystem(){
    
}

MessageSystem::~MessageSystem(){
    
}

bool MessageSystem::publish( std::string_view topic, const void* data, size_t size){
    uint64_t topic_id = hash::hash64(std::string(topic));


}