package network.cere.ddc.client.common

import network.cere.ddc.client.common.signer.Sr25519Signer
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import kotlin.test.assertEquals

internal class Sr25519SignerTest {

    @ParameterizedTest
    @CsvSource(
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60,85f4460e723da28da7d20dad261d4e89737e0daf93b53954b638a1fc540eb33c620e9e49dba7c0ffd1b8c21ee66f0318e3f8ffa9dafd3c223fa645f9c8960a07",
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f607b32691970,85f4460e723da28da7d20dad261d4e89737e0daf93b53954b638a1fc540eb33c620e9e49dba7c0ffd1b8c21ee66f0318e3f8ffa9dafd3c223fa645f9c8960a07",
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a,3d6c47a01a949b74f97582b55bc4775c6c3c73e23545436a2785940df824abeb57eb6d607a230d6f2bedbf3bb13eeb2a83bb8915710d6906d1e6b2e45d150d04"
    )
    fun `Sr25519 signer - sign (positive scenario)`(privateKey: String, expected: String) {
        //given
        val data = "test_string"

        //when
        val result = Sr25519Signer(privateKey).sign(data)

        //then
        assertEquals(result, expected)
    }
}