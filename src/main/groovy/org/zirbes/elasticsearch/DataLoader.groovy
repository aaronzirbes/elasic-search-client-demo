package org.zirbes.elasticsearch

class DataLoader {

    static List<Thing> loadThings() {

        String thingText = this.class.getResourceAsStream("/things.txt").text

        List<Thing> things = []

        thingText.eachLine{ String line ->

            // 0         1         2         3
            // 01234567890123456789012345678901234567
            // ID     LAT    LON     Location
            // [CGX]  41.87   87.62  Chicago/Meigs,IL

            String code      = line[1..3].trim()
            Float lat       = line[7..11].trim().toFloat()
            Float lon       = line[14..19].trim().toFloat()
            String cityState = line[22..-1].trim()
            String city      = cityState[0..-4]
            String state     = cityState[-2..-1]

            things << new Thing(
                key: code,
                name: "${code} in ${cityState}",
                city: city,
                state: state,
                location: new Location(lat, lon)
            )
        }

        return things

    }

}
