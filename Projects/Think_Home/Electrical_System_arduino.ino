/*****IR & PIR SENSOR SETUP*********/
int OutPin = 5;
int OutPin1 = 6;
int OutPinIR = 9;
int OutPinIR1 = 10;
int inputPin = 3;
int inputPin1 = 4;
int inputPinIR = 7;
int inputPinIR1 = 8;
int pirState = LOW;
int val = 0, val1 = 0, i = 0, j = 0;
/******************************/
/**********FAN CONTROL*********/
int fin = 1;
int fout=11;
/******************************/
int c1 = 0, c2 = 0, b1 = 0, b2 = 0;
void setup()
{
  pinMode(OutPin, OUTPUT);
  pinMode(OutPin1, OUTPUT);
  pinMode(inputPin, INPUT);
  pinMode(inputPin1, INPUT);
  pinMode(OutPinIR, OUTPUT);
  pinMode(OutPinIR1, OUTPUT);
  pinMode(inputPinIR, INPUT);
  pinMode(inputPinIR1, INPUT);
  pinMode(fout, OUTPUT);
  pinMode(12, INPUT);
  Serial.begin(9600);
  Serial.print("YESSS");
}

void loop()
{
/************* FAN COTROL STARTED *******************************/
    fin = digitalRead(12);
    if(fin == 0)
    {
      digitalWrite(fout,LOW);
    }
    else if(fin == 1) 
    {
      digitalWrite(fout, HIGH);
    }
/************* FAN CONTROL ENDED ************************/
/************* PIR Control Started **********************/
    val = digitalRead(inputPin);
    val1 = digitalRead(inputPin1);
    j = digitalRead(inputPinIR1);
    if (val == HIGH)
    {
      if (pirState == LOW)
      {
        if(!c1)
        { 
          digitalWrite(OutPin, LOW);
          c1 = 1;
          Serial.println("Enter BED ROOM 1");
          //digitalWrite(12, HIGH);
          delay(4000);
          //digitalWrite(12, LOW);
          //delay(4000);
        }
        else
        {
          Serial.println("Exited BED ROOM 1");
          digitalWrite(OutPin, HIGH);
          c1 = 0;
        }
        pirState = HIGH;
      }
      else
      {
        if (pirState == HIGH)
        {
          pirState = LOW;
        }
      }
    }
    if (val1 == HIGH)
    {
      if (pirState == LOW)
      {
        if(!c2)
        { 
           digitalWrite(OutPin1, LOW);
           c2 = 1;
           Serial.println("Enterd BED ROOM 2");
           //digitalWrite(12, HIGH);
           delay(4000);
           //digitalWrite(12, LOW);
           //delay(4000);
        }
        else
        {
           Serial.println("Exited BED ROOM 2");
           digitalWrite(OutPin1, HIGH);
           c2 = 0;
           delay(4000);
        }
        pirState = HIGH;
      }
      else
      {
        if (pirState == HIGH)
        {
          pirState = LOW;
        }
      }
    }
    if(j == HIGH)
    {
      if(pirState==LOW)
      {
      if(!b2)
      {
        digitalWrite(OutPinIR1, LOW);
        b2 = 1;
        Serial.println("Entered Bathroom 2");
        delay(4000);
      }
      else
      {
        Serial.println("Exited Bathroom 2");
        digitalWrite(OutPinIR1, HIGH);
        b2 = 0;
        delay(4000);
      }
      pirState = HIGH;
      }
      else
      {
        if (pirState == HIGH)
        {
          pirState = LOW;
        }
      }
      
    }
/************* PIR Control finished **********************/
/************* IR Control Started ************************/
    i = digitalRead(inputPinIR);
   
    if(i == LOW)
    {
      if(!b1)
      {
        digitalWrite(OutPinIR, LOW);
        b1 = 1;
        Serial.println("Entered Bathroom 1");
        delay(4000);
      }
      else
      {
        Serial.println("Exited Bathroom 1");
        digitalWrite(OutPinIR, HIGH);
        b1 = 0;
        delay(4000);
      }
    }
/************* IR Control finished*************************/    
}
