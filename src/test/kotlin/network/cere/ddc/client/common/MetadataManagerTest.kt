package network.cere.ddc.client.common

import io.netty.handler.codec.http.HttpResponseStatus
import io.smallrye.mutiny.Uni
import io.vertx.ext.web.client.impl.HttpResponseImpl
import io.vertx.mutiny.core.buffer.Buffer
import io.vertx.mutiny.ext.web.client.HttpRequest
import io.vertx.mutiny.ext.web.client.HttpResponse
import io.vertx.mutiny.ext.web.client.WebClient
import io.vertx.mutiny.ext.web.codec.BodyCodec
import network.cere.ddc.client.api.AppTopology
import network.cere.ddc.client.api.NodeMetadata
import network.cere.ddc.client.api.PartitionTopology
import network.cere.ddc.client.common.exception.AppTopologyLoadException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.*
import java.net.ConnectException
import kotlin.test.assertEquals

internal class MetadataManagerTest {


    private companion object {
        private const val APP_PUB_KEY = "appPubKey"
        private const val USER_PUB_KEY = "userPubKey"
    }

    private val client: WebClient = mock()
    private val testSubject = MetadataManager(listOf("bootstrapNode-1"), client, 1, 5)


    @Test
    fun `Metadata Manager - one time getAppTopology`() {
        //given
        val clientResp =
            generateClientScenario(Uni.createFrom().item(generateSuccessResponse(AppTopology(APP_PUB_KEY))))
        whenever(client.getAbs("bootstrapNode-1/api/rest/apps/${APP_PUB_KEY}/topology")) doReturn clientResp

        //when
        val result = testSubject.getAppTopology(APP_PUB_KEY)

        //then
        assertEquals(result, AppTopology(APP_PUB_KEY))
        verify(client).getAbs("bootstrapNode-1/api/rest/apps/${APP_PUB_KEY}/topology")
    }

    @Test
    fun `Metadata Manager - getAppTopology with first failed`() {
        //given
        val clientResp = generateClientScenario(Uni.createFrom().failure(ConnectException()))
        whenever(client.getAbs("bootstrapNode-1/api/rest/apps/${APP_PUB_KEY}/topology")) doReturn clientResp

        //when
        val action = { testSubject.getAppTopology(APP_PUB_KEY) }

        //then
        assertThrows<AppTopologyLoadException> { action() }
        verify(client, times(2)).getAbs("bootstrapNode-1/api/rest/apps/${APP_PUB_KEY}/topology")
    }

    @Test
    fun `Metadata Manager - getAppTopology with rebuild order after failed`() {
        //given
        val testSubject = MetadataManager(listOf("bootstrapNode-1", "bootstrapNode-2"), client, 1, 5)
        val failResp = generateClientScenario(Uni.createFrom().failure(ConnectException()))
        val successResp =
            generateClientScenario(Uni.createFrom().item(generateSuccessResponse(AppTopology(APP_PUB_KEY))))
        whenever(client.getAbs("bootstrapNode-2/api/rest/apps/${APP_PUB_KEY}/topology")) doAnswer { successResp }
        whenever(client.getAbs("bootstrapNode-1/api/rest/apps/${APP_PUB_KEY}/topology")) doReturn failResp

        //when
        val resultFirst = testSubject.getAppTopology(APP_PUB_KEY)
        val resultSecond = testSubject.getAppTopology(APP_PUB_KEY)

        //then
        assertEquals(resultFirst, AppTopology(APP_PUB_KEY))
        assertEquals(resultSecond, AppTopology(APP_PUB_KEY))

        verify(client).getAbs("bootstrapNode-1/api/rest/apps/${APP_PUB_KEY}/topology")
        verify(client, times(2)).getAbs("bootstrapNode-2/api/rest/apps/${APP_PUB_KEY}/topology")
    }

    @Test
    fun `Metadata Manager - getAppTopology from detected node`() {
        //given
        val appTopology = AppTopology(
            APP_PUB_KEY, listOf(
                PartitionTopology(
                    master = NodeMetadata(nodeHttpAddress = "bootstrapNode-1"),
                    replicas = setOf(NodeMetadata(nodeHttpAddress = "addedNode"))
                )
            )
        )
        val secondAppTopology = appTopology.copy(
            partitions = listOf(
                PartitionTopology(
                    master = NodeMetadata(nodeHttpAddress = "addedNode"),
                    replicas = setOf(NodeMetadata(nodeHttpAddress = "bootstrapNode-1"))
                )
            )
        )

        val failResp = generateClientScenario(Uni.createFrom().failure(ConnectException()))
        val successResp = generateClientScenario(Uni.createFrom().item(generateSuccessResponse(appTopology)))
        val newSuccessResp = generateClientScenario(Uni.createFrom().item(generateSuccessResponse(secondAppTopology)))
        whenever(client.getAbs("bootstrapNode-1/api/rest/apps/${APP_PUB_KEY}/topology")) doReturn successResp doReturn failResp
        whenever(client.getAbs("addedNode/api/rest/apps/${APP_PUB_KEY}/topology")) doReturn newSuccessResp

        //when
        val resultFirst = testSubject.getAppTopology(APP_PUB_KEY)
        val resultSecond = testSubject.getAppTopology(APP_PUB_KEY)

        //then
        val expectedFirst = AppTopology(
            APP_PUB_KEY, listOf(
                PartitionTopology(
                    master = NodeMetadata(nodeHttpAddress = "bootstrapNode-1"),
                    replicas = setOf(NodeMetadata(nodeHttpAddress = "addedNode"))
                )
            )
        )
        val expectedSecond = appTopology.copy(
            partitions = listOf(
                PartitionTopology(
                    master = NodeMetadata(nodeHttpAddress = "addedNode"),
                    replicas = setOf(NodeMetadata(nodeHttpAddress = "bootstrapNode-1"))
                )
            )
        )

        assertEquals(resultFirst, expectedFirst)
        assertEquals(resultSecond, expectedSecond)

        verify(client, times(2)).getAbs("bootstrapNode-1/api/rest/apps/${APP_PUB_KEY}/topology")
        verify(client).getAbs("addedNode/api/rest/apps/${APP_PUB_KEY}/topology")
    }

    @Test
    fun `Metadata Manager - retry on bad response for getAppTopology`() {
        //given
        val clientResp = generateClientScenario(Uni.createFrom().item(generateBadResponse()))
        whenever(client.getAbs("bootstrapNode-1/api/rest/apps/${APP_PUB_KEY}/topology")) doAnswer { clientResp }

        //when
        val action = { testSubject.getAppTopology(APP_PUB_KEY) }

        //then
        assertThrows<AppTopologyLoadException> { action() }
        verify(client, times(2)).getAbs("bootstrapNode-1/api/rest/apps/${APP_PUB_KEY}/topology")
    }

    @Test
    fun `Metadata Manager - getProducerTargetNode`() {
        //given
        val appTopology = AppTopology(
            partitions = listOf(
                PartitionTopology(
                    active = true,
                    sectorStart = 943184635,
                    sectorEnd = 943184636,
                    master = NodeMetadata(nodeHttpAddress = "nodeAddress")
                )
            )
        )

        //when
        val result = testSubject.getProducerTargetNode(USER_PUB_KEY, appTopology)

        //then
        assertEquals(result, "nodeAddress")
    }

    @Test
    fun `Metadata Manager - getConsumerTargetPartitions`() {
        //given
        val appTopology = AppTopology(
            partitions = listOf(
                PartitionTopology(
                    active = true,
                    sectorStart = 943184635,
                    sectorEnd = 943184636,
                    master = NodeMetadata(nodeHttpAddress = "nodeAddress")
                )
            )
        )

        //when
        val result = testSubject.getConsumerTargetPartitions(USER_PUB_KEY, appTopology)

        //then
        val expected = listOf(
            PartitionTopology(
                active = true,
                sectorStart = 943184635,
                sectorEnd = 943184636,
                master = NodeMetadata(nodeHttpAddress = "nodeAddress")
            )
        )
        assertEquals(result, expected)
    }

    private fun generateClientScenario(response: Uni<HttpResponse<AppTopology>>): HttpRequest<Buffer> {
        val onSendMock: HttpRequest<AppTopology> = mock { on { send() } doReturn response }
        return mock { on { `as`(any<BodyCodec<*>>()) } doReturn onSendMock }
    }

    private fun generateSuccessResponse(appTopology: AppTopology) = HttpResponse.newInstance<AppTopology>(
        HttpResponseImpl(
            null, HttpResponseStatus.OK.code(), HttpResponseStatus.OK.reasonPhrase(), null,
            null, listOf(), appTopology, listOf()
        )
    )

    private fun generateBadResponse() = HttpResponse.newInstance<AppTopology>(
        HttpResponseImpl(
            null, HttpResponseStatus.FORBIDDEN.code(), HttpResponseStatus.FORBIDDEN.reasonPhrase(), null,
            null, listOf(), null, listOf()
        )
    )

}