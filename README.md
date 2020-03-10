AddArrayRecord
AddArrayRecord es un procesador que agrega un campo de tipo record y traslada informacion de la estructura principal a este nuevo campo para dejar un historico.
si se configuran los campos Field1,Field3 : 

/iN:

[ {
  "Field1" : "1",
  "Field2" : "2",
  "Field3" : "3"
} ]

/OUT:

[ {
  "Field1" : "1",
  "Field2" : "2",
  "Field3" : "3",
  "HISTORY" : [ {
    "Field1" : "1",
    "Field3" : "3"
  } ]
} ]
