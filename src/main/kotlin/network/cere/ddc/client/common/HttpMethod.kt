package network.cere.ddc.client.common

sealed class HttpMethod() {
    abstract fun getMethodName(): String
}

class Get(): HttpMethod() {
    private val name = "GET"

    override fun getMethodName(): String {
        return name
    }
}

class Post(): HttpMethod() {
    private val name = "POST"

    override fun getMethodName(): String {
        return name
    }
}

class Put(): HttpMethod() {
    private val name = "PUT"

    override fun getMethodName(): String {
        return name
    }
}

class Update(): HttpMethod() {
    private val name = "UPDATE"

    override fun getMethodName(): String {
        return name
    }
}

class Delete(): HttpMethod() {
    private val name = "DELETE"

    override fun getMethodName(): String {
        return name
    }
}
