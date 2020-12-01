package is.hail.compatibility

import is.hail.asm4s.{Code, LineNumber}
import is.hail.io.compress.LZ4
import is.hail.io._

final case class LZ4BlockBufferSpec(blockSize: Int, child: BlockBufferSpec)
    extends LZ4BlockBufferSpecCommon {
  def lz4 = LZ4.hc
  def stagedlz4(implicit line: LineNumber): Code[LZ4] =
    Code.invokeScalaObject0[LZ4](LZ4.getClass, "hc")
  def typeName = "LZ4BlockBufferSpec"
}

