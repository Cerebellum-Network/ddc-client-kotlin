package network.cere.ddc.client.common

sealed class HttpMethod() {
    abstract fun getMethodName(): String
}

object Get: HttpMethod() {
    private const val name = "GET"

    override fun getMethodName(): String {
        return name
    }
}

object Post: HttpMethod() {
    private const val name = "POST"

    override fun getMethodName(): String {
        return name
    }
}

object Put: HttpMethod() {
    private const val name = "PUT"

    override fun getMethodName(): String {
        return name
    }
}

object Update: HttpMethod() {
    private const val name = "UPDATE"

    override fun getMethodName(): String {
        return name
    }
}

object Delete: HttpMethod() {
    private const val name = "DELETE"

    override fun getMethodName(): String {
        return name
    }
}
