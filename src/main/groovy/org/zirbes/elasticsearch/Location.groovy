package org.zirbes.elasticsearch

import groovy.transform.CompileStatic
import groovy.transform.ToString

@CompileStatic
@ToString
class Location implements Serializable {

    BigDecimal lat
    BigDecimal lon

    Location() {
    }

    Location(BigDecimal lat, BigDecimal lon) {
        this.lat = lat
        this.lon = lon
    }

}
