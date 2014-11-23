package org.zirbes.elasticsearch

import groovy.transform.CompileStatic
import groovy.transform.ToString

@CompileStatic
@ToString
class Thing {
    String key
    String name
    String email
    String city
    String state
    Location location
}
