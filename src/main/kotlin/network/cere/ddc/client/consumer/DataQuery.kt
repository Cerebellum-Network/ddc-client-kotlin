package network.cere.ddc.client.consumer

data class DataQuery(
   val from: String,
   val to: String,
   val fields: List<String>
)
