package cn.com.aeonlife.etl.domain



//这里只是定义了3个参数，定义过多看起来不太方便就没写那么多
class CustomerBean(f_key: String,
                   f_inserttime: String,
                   f_upDatetime: String,
                   f_isobsolete: Int,
                   f_code: String,
                   f_specialflag: String,
                   f_selfdata: Int,
                   f_branch: Int,
                   f_commender: Int,
                   f_name: String,
                   f_nationality: Int,
                   f_gender: Int,
                   f_birthday: String,
                   f_age: Int,
                   f_marriage: Int,
                   f_education: Int,
                   f_documenttype: Int,
                   f_document: String,
                   f_email: String,
                   f_mobile: String,
                   f_companyname: String,
                   f_companycity: Int,
                   f_companydistrict: Int,
                   f_companyaddress: String
                  ) extends Product {
  //进行模式匹配
  override def productElement(n: Int): Any = n match {
    case 0 => f_key //参数的下表 对应的参数
    case 1 => f_inserttime
    case 2 => f_upDatetime
    case 3 => f_isobsolete //参数的下表 对应的参数
    case 4 => f_code
    case 5 => f_specialflag
    case 6 => f_selfdata //参数的下表 对应的参数
    case 7 => f_branch
    case 8 => f_commender
    case 9 => f_name //参数的下表 对应的参数
    case 10 => f_nationality
    case 11 => f_gender
    case 12 => f_birthday //参数的下表 对应的参数
    case 13 => f_age
    case 14 => f_marriage
    case 15 => f_education
    case 16 => f_documenttype
    case 17 => f_document
    case 18 => f_email
    case 19 => f_mobile
    case 20 => f_companyname
    case 21 => f_companycity
    case 22 => f_companydistrict
    case 23 => f_companyaddress
    case _ => throw new IndexOutOfBoundsException(n.toString())
  }

  //返回的是传参的个数
  override def productArity: Int = 24

  //自定义比较 ，是否包含这个数
  override def canEqual(that: Any): Boolean = that.isInstanceOf[CustomerBean]

  override def toString: String = "测试tostring"


}