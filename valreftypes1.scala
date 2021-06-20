package org.inceptez.scalaprograms
// Function based / OOPS (Spark framework)
//1. Variables/Values/simple types, conditional, control statements
//2. Methods/Functions
//3. Collections
//4. OOPS, methods predefined/userdefined
// I won't be providing recordings
// Raise questions at any point voice, chat, address immediate/later before break..

object valrefandtypes11 {
    //main method is the entry point of the scala program
  
  def main(args:Array[String])
  {
      //semicolon is optional
    //single line and /* */ multi line comments

        println("Welcome to Scala...");   
        println("Welcome to Inceptez..")
    
        /*Two types of variables
    		1. Mutable Variable - can change value
    		2. Immutable Variable - only assign value, cannot change 
    * */
    
    val bonus:Int = 1000
    var salary:Int = 20000
    
   //bonus = 2000
    
    //Not allowed to reassign value because declared as val
    
    //Able to reassign value because declared as var
    
    salary = salary+bonus;
    
    println("Variable Types..")
    
    
    /*Type Inference - compiler can infer the datatype based on the assignment 
    
    * */	
    println("Type Inference..")
    val a = 10
    var b = 20.0

    //statically typed -  each variable and expression is already known at compile time
    //cannot assign different datatype once the value is assigned
    var name = "Inceptez"
    name = "Inceptez Technologies Pvt Ltd"
    println(name)
    //name = 25
    println("Statically Type..")

    
    /*
    
    In Scala, there are 2 different value types
    
    1. value type
    2. reference type 
    */
      
    
    //******DataType - valuetype**************/
      
    
    //Byte: Byte is an 8 bit signed value.The value ranges from -128 to 127
    val num0:Byte = 100
    println("Num0 of Byte type is " + num0);
    //Short: is an 16 bit signed value ranging from -32768 to 32767
    val num1:Short = 10
    
    //Int: is an 32 bit signed value ranging from -2147483648 to 2147483647
    val num2:Int = 100
    
    //Long: is a 64 bit signed value ranging from -9223372036854775808 to 9223372036854775807
    val num3:Long = 100
    
    //Float: is a 32 bit floating point ranges 6-7 decimal digits precision
    val num4:Float = 250.25f
    
    //Double: is a 64 bit double with 15-16 decimal digits precision
    val num5:Double = 250.25
    
    //Boolean: literal with true and false values
    val bstr:Boolean = true
    
    //Char: is a 16 bit unsigned unicode character ranging from U+0000 to U+FFFF.
    val cstr:Char = 'A'
    
    //Unit: Corresponds to no value
    val str4:Unit =()

    //AnyVal: is the root class of all value types
   
    val av1:AnyVal = 10
    val av2:AnyVal = 10.0
    val av3:AnyVal = 'D'
    val av4:AnyVal = ()
    
    val a1:AnyVal = 10
  
    /******DataTypes - Reference type**************/
        
    //String: A sequence of characters enclosed with double quotes.
    
     
    val str2:AnyRef = "pkg.class@xyb3223bdxx"
    var str22:AnyVal = 10
    //All custom objects are reference type 
    
    /******DataTypes - Any type**************/
    
    //Any: Super type of any value or reference type
    val atype1:Any = "Inceptez"
    val atype2:Any = 100
    val atype3:Any = 200.50
    val atype4:Any = 'G'
    val atype5:Any = null
               
  }
  
  }



