from dataclasses import dataclass, field
from typing import Dict, Any, List, Union
from enum import Enum

@dataclass
class KB_BIT_DATA:  
    user_name: str = ""
    bit_size: int = 1
    flags: Dict[str, Any] = field(default_factory=dict)
    flags_mask: Dict[str, int] = field(default_factory=dict)
    flag_data: Dict[str, Any] = field(default_factory=dict)
    flag_change: Dict[str, bool] = field(default_factory=dict)
    bit_mask: int = 0
    node_id: str = ""


class TokenType(Enum):
    LPAREN = "LPAREN"
    RPAREN = "RPAREN"
    OPERATOR = "OPERATOR"
    REFERENCE = "REFERENCE"


@dataclass
class Token:
    type: TokenType
    value: str


@dataclass
class SExpNode:
    """Represents a node in the parsed S-expression tree"""
    operator: str
    operands: List[Union['SExpNode', str]]


class SExpressionProcessor:
    """
    Processes S-expressions for KB_BIT_DATA flag operations.
    
    Syntax: (operator user_name:flag_name ...)
    
    Operators:
    - bit_changed: Returns True if all referenced flags have changed
    - and: Returns True if all referenced flags are 1
    - or: Returns True if any referenced flag is 1
    """
    
    VALID_OPERATORS = {'bit_changed', 'and', 'or'}
    
    def __init__(self):
        self.tokens = []
        self.position = 0
    
    def tokenize(self, s_expression: str) -> List[Token]:
        """
        Tokenize an S-expression into a list of tokens.
        
        Args:
            s_expression: S-expression string like "(and user1:flag1 user2:flag2)"
            
        Returns:
            List of Token objects
            
        Example:
            >>> processor = SExpressionProcessor()
            >>> tokens = processor.tokenize("(and user1:flag1 user2:flag2)")
        """
        tokens = []
        i = 0
        s_expression = s_expression.strip()
        
        while i < len(s_expression):
            char = s_expression[i]
            
            # Skip whitespace
            if char.isspace():
                i += 1
                continue
            
            # Left parenthesis
            if char == '(':
                tokens.append(Token(TokenType.LPAREN, '('))
                i += 1
            
            # Right parenthesis
            elif char == ')':
                tokens.append(Token(TokenType.RPAREN, ')'))
                i += 1
            
            # Symbol (operator or reference)
            else:
                # Collect the symbol
                start = i
                while i < len(s_expression) and not s_expression[i].isspace() and s_expression[i] not in '()':
                    i += 1
                
                symbol = s_expression[start:i]
                
                # Determine if it's an operator or reference
                if symbol in self.VALID_OPERATORS:
                    tokens.append(Token(TokenType.OPERATOR, symbol))
                elif ':' in symbol:
                    tokens.append(Token(TokenType.REFERENCE, symbol))
                else:
                    raise ValueError(f"Invalid symbol: '{symbol}'. Must be an operator or user_name:flag_name reference.")
        
        return tokens
    
    def _parse_tokens(self, tokens: List[Token]) -> SExpNode:
        """Parse tokens into an S-expression tree"""
        self.tokens = tokens
        self.position = 0
        return self._parse_expression()
    
    def _parse_expression(self) -> Union[SExpNode, str]:
        """Parse a single expression (either an atom or a list)"""
        if self.position >= len(self.tokens):
            raise ValueError("Unexpected end of expression")
        
        token = self.tokens[self.position]
        
        if token.type == TokenType.LPAREN:
            # Parse list: (operator operand1 operand2 ...)
            self.position += 1
            
            if self.position >= len(self.tokens):
                raise ValueError("Expected operator after '('")
            
            operator_token = self.tokens[self.position]
            if operator_token.type != TokenType.OPERATOR:
                raise ValueError(f"Expected operator, got {operator_token.value}")
            
            operator = operator_token.value
            self.position += 1
            
            operands = []
            while self.position < len(self.tokens) and self.tokens[self.position].type != TokenType.RPAREN:
                operands.append(self._parse_expression())
            
            if self.position >= len(self.tokens):
                raise ValueError("Missing closing parenthesis")
            
            self.position += 1  # Consume RPAREN
            
            return SExpNode(operator=operator, operands=operands)
        
        elif token.type == TokenType.REFERENCE:
            # Parse reference: user_name:flag_name
            self.position += 1
            return token.value
        
        else:
            raise ValueError(f"Unexpected token: {token.value}")
    
    def execute(self, s_expression: Union[str, List[Token]], kb_data: Dict[str, KB_BIT_DATA]) -> bool:
        """
        Execute a tokenized S-expression against KB_BIT_DATA dictionary.
        
        Args:
            s_expression: Either a string S-expression or list of tokens
            kb_data: Dictionary mapping user_name to KB_BIT_DATA objects
            
        Returns:
            Boolean result of the expression evaluation
            
        Example:
            >>> processor = SExpressionProcessor()
            >>> kb_data = {
            ...     "user1": KB_BIT_DATA(user_name="user1", 
            ...                          flag_data={"flag1": 1}, 
            ...                          flag_change={"flag1": True})
            ... }
            >>> result = processor.execute("(bit_changed user1:flag1)", kb_data)
        """
        # If string provided, tokenize first
        if isinstance(s_expression, str):
            tokens = self.tokenize(s_expression)
        else:
            tokens = s_expression
        
        # Parse tokens into expression tree
        expr_tree = self._parse_tokens(tokens)
        
        # Evaluate the expression tree
        return self._evaluate(expr_tree, kb_data)
    
    def _evaluate(self, node: Union[SExpNode, str], kb_data: Dict[str, KB_BIT_DATA]) -> bool:
        """Recursively evaluate an expression tree node"""
        
        # If it's a leaf node (reference), look up the value
        if isinstance(node, str):
            return self._lookup_reference(node, kb_data, need_value=True)
        
        # Otherwise, it's an operator node
        operator = node.operator
        operands = node.operands
        
        if operator == 'bit_changed':
            # Return True if all referenced flags have changed
            return all(self._check_bit_changed(op, kb_data) for op in operands)
        
        elif operator == 'and':
            # Return True if all operands evaluate to True
            return all(self._evaluate(op, kb_data) for op in operands)
        
        elif operator == 'or':
            # Return True if any operand evaluates to True
            return any(self._evaluate(op, kb_data) for op in operands)
        
        else:
            raise ValueError(f"Unknown operator: {operator}")
    
    def _check_bit_changed(self, operand: Union[SExpNode, str], kb_data: Dict[str, KB_BIT_DATA]) -> bool:
        """Check if a bit has changed"""
        if isinstance(operand, str):
            # It's a reference
            return self._lookup_reference(operand, kb_data, need_change=True)
        else:
            # It's a nested expression - evaluate it first
            # For bit_changed, we need to check if the result represents a changed state
            return self._evaluate(operand, kb_data)
    
    def _lookup_reference(self, reference: str, kb_data: Dict[str, KB_BIT_DATA], 
                         need_value: bool = False, need_change: bool = False) -> bool:
        """
        Look up a reference (user_name:flag_name) in the KB data.
        
        Args:
            reference: String in format "user_name:flag_name"
            kb_data: Dictionary of KB_BIT_DATA objects
            need_value: If True, return flag_data value (1 = True, 0 = False)
            need_change: If True, return flag_change value
        """
        if ':' not in reference:
            raise ValueError(f"Invalid reference format: '{reference}'. Expected 'user_name:flag_name'")
        
        user_name, flag_name = reference.split(':', 1)
        
        if user_name not in kb_data:
            raise KeyError(f"User '{user_name}' not found in KB data")
        
        kb_entry = kb_data[user_name]
        
        if need_change:
            if flag_name not in kb_entry.flag_change:
                raise KeyError(f"Flag '{flag_name}' not found in flag_change for user '{user_name}'")
            return kb_entry.flag_change[flag_name]
        
        if need_value:
            if flag_name not in kb_entry.flag_data:
                raise KeyError(f"Flag '{flag_name}' not found in flag_data for user '{user_name}'")
            return kb_entry.flag_data[flag_name] == 1
        
        # Default: return flag_data value
        if flag_name not in kb_entry.flag_data:
            raise KeyError(f"Flag '{flag_name}' not found in flag_data for user '{user_name}'")
        return kb_entry.flag_data[flag_name] == 1


# Example usage
if __name__ == "__main__":
    # Create test data
    kb_data = {
        "user1": KB_BIT_DATA(
            user_name="user1",
            flag_data={"flag1": 1, "flag2": 0},
            flag_change={"flag1": True, "flag2": False}
        ),
        "user2": KB_BIT_DATA(
            user_name="user2",
            flag_data={"flag1": 1, "flag2": 1},
            flag_change={"flag1": True, "flag2": True}
        ),
        "user3": KB_BIT_DATA(
            user_name="user3",
            flag_data={"flag1": 0},
            flag_change={"flag1": False}
        )
    }
    
    processor = SExpressionProcessor()
    
    # Test bit_changed
    print("Test bit_changed:")
    result = processor.execute("(bit_changed user1:flag1 user2:flag1)", kb_data)
    print(f"  (bit_changed user1:flag1 user2:flag1) = {result}")  # True (both changed)
    
    result = processor.execute("(bit_changed user1:flag1 user1:flag2)", kb_data)
    print(f"  (bit_changed user1:flag1 user1:flag2) = {result}")  # False (flag2 didn't change)
    
    # Test and
    print("\nTest and:")
    result = processor.execute("(and user1:flag1 user2:flag1)", kb_data)
    print(f"  (and user1:flag1 user2:flag1) = {result}")  # True (both are 1)
    
    result = processor.execute("(and user1:flag1 user1:flag2)", kb_data)
    print(f"  (and user1:flag1 user1:flag2) = {result}")  # False (flag2 is 0)
    
    # Test or
    print("\nTest or:")
    result = processor.execute("(or user1:flag1 user1:flag2)", kb_data)
    print(f"  (or user1:flag1 user1:flag2) = {result}")  # True (flag1 is 1)
    
    result = processor.execute("(or user3:flag1 user1:flag2)", kb_data)
    print(f"  (or user3:flag1 user1:flag2) = {result}")  # False (both are 0)
    
    # Test nested expressions
    print("\nTest nested:")
    result = processor.execute("(and (or user1:flag1 user3:flag1) user2:flag2)", kb_data)
    print(f"  (and (or user1:flag1 user3:flag1) user2:flag2) = {result}")  # True
    
    result = processor.execute("(or (and user1:flag1 user1:flag2) user2:flag1)", kb_data)
    print(f"  (or (and user1:flag1 user1:flag2) user2:flag1) = {result}")  # True