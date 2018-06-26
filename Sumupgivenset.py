def list_return_function(num): 
  i = 0
  new_set  = []
  while i < (len(num)-1):
    print(i)
    j = 1
    while j < (len(num)):
        print("hi")
        sum = num[i] + num [j]
        suminnum = sum in num
        suminnewset = sum in new_set
        print(suminnum)
        if suminnum == False:
          if suminnewset == False:
            new_set.append(sum)
        print(new_set)
        j +=1    
    i +=1
  print(new_set)  
  return new_set
