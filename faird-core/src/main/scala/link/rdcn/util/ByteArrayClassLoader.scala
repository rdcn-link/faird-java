/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/31 16:11
 * @Modified By:
 */
package link.rdcn.util

class ByteArrayClassLoader(classBytes: Map[String, Array[Byte]], parent: ClassLoader) extends ClassLoader(parent) {
  override def findClass(name: String): Class[_] = {
    // 检查当前类加载器是否已经加载过这个类
    val loadedClass = findLoadedClass(name)
    if (loadedClass != null) {
      return loadedClass
    }

    // 尝试从传入的字节码 Map 中查找
    classBytes.get(name) match {
      case Some(bytes) =>
        defineClass(name, bytes, 0, bytes.length)
      case None =>
        super.findClass(name)
    }
  }
}