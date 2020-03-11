package is.hail.rvd

import is.hail.annotations.{Region, RegionHandle, RegionValueBuilder}

import scala.collection.mutable

//object RVDContext {
//  def default: RVDContext = new RVDContext(Region())
//
//  def fromRegion(region: Region): RVDContext = new RVDContext(region)
//}

class RVDContext(val partitionRegion: RegionHandle, val r: RegionHandle) {
  def region: RegionHandle = r
}

//class RVDContextOld(val r: Region) extends AutoCloseable {
//  private[this] val children = new mutable.HashSet[AutoCloseable]()
//
//  private[this] def own(child: AutoCloseable): Unit = children += child
//  private[this] def disown(child: AutoCloseable): Unit =
//    assert(children.remove(child))
//
//  own(r)
//
//  def freshContext: RVDContextOld = {
//    val ctx = RVDContext.default
//    own(ctx)
//    ctx
//  }
//
//  def freshRegion: Region = {
//    val r2 = Region()
//    own(r2)
//    r2
//  }
//
//  def region: Region = r
//
//  private[this] val theRvb = new RegionValueBuilder(r)
//  def rvb = theRvb
//
//  // frees the memory associated with this context
//  def close(): Unit = {
//    var e: Exception = null
//    children.foreach { child =>
//      try {
//        child.close()
//      } catch {
//        case e2: Exception =>
//          if (e == null)
//            e = e2
//          else
//            e.addSuppressed(e2)
//      }
//    }
//
//    if (e != null)
//      throw e
//  }
//
//  def closeChild(child: AutoCloseable): Unit = {
//    child.close()
//    disown(child)
//  }
//}
