package network.cere.ddc.client.common.exception

class AppTopologyLoadException(message: String, val address: String?, throwable: Throwable?) :
    RuntimeException(message, throwable) {

    constructor(address: String, message: String) : this(message, address, null)
    constructor(message: String, throwable: Throwable?) : this(message, null, throwable)
}