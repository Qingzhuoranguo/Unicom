#pragma once

#include "message_system.h"

#include <string>

class Component {
public:
    Component();
    ~Component();

    // virtual bool onStart() = 0;


private:
    MessageSystem *m_msgSys;

    std::string m_componentName;
    
};